#include <server_lib/log_accumulator.h>

#include <server_lib/asserts.h>
#include <server_lib/logging_helper.h>

#include <iostream>

namespace server_lib {

log_accumulator::log_accumulator()
    : _flush_active(false)
    , _new_set_force_flush(false)
    , _execute(true)
{
    _logs.resize(2);
    _active_container_p = &(_logs[0]);
    _flush_container_p = &(_logs[1]);
}

log_accumulator::~log_accumulator()
{
    if (_execute)
    {
        _execute.store(false);
        if (_thd.joinable())
            _thd.join();
    }

    flush();
}

void log_accumulator::init(size_t flush_period_ms, size_t limit_by_thread, size_t throttling_time_ms, size_t wait_flush,
                           size_t pre_init_logs_limit)
{
#if defined(_USE_LOG_ACCUMULATOR)

    _flush_period_ms.store(flush_period_ms);
    _limit_by_thread.store(limit_by_thread);
    _throttling_time_ms.store(throttling_time_ms);
    _wait_flush.store(wait_flush);

    LOG_INFO("Logger Accumulator init. Flush period ms: " << flush_period_ms << ", limit logs by thread before "
             << "throttling: " << limit_by_thread << ", throttling time in ms(for heavily spammy threads): "
             << throttling_time_ms);

    if (_thd.joinable())
        return;

    release_logs_pre_init(pre_init_logs_limit);

    _thd = std::thread([this]() {
        while (_execute.load())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(_flush_period_ms.load()));
            try
            {
                if (!logger::instance().get_force_flush())
                    flush();
            }
            catch (const std::exception& e)
            {
                LOG_ERROR(e.what());
            }
        }
    });

#endif
}

void log_accumulator::put(logger::log_message&& msg)
{
    if (logger::instance().get_force_flush())
    {
        if (!_new_set_force_flush)
        {
            static std::mutex guard;
            const std::lock_guard<std::mutex> lock(guard);

            while (_flush_active)
                std::this_thread::sleep_for(std::chrono::milliseconds(_wait_flush));

            add_log_msg(std::move(msg));

            flush();
            flush();

            _new_set_force_flush = true;
        }

        logger::instance().write(msg);
        return;
    }

    _new_set_force_flush = false;

    size_t count_by_thread = (*_active_container_p)[std::this_thread::get_id()].size();

    add_log_msg(std::move(msg));

    if (count_by_thread >= _limit_by_thread)
        std::this_thread::sleep_for(std::chrono::milliseconds(_throttling_time_ms));
}

void log_accumulator::add_log_msg(logger::log_message&& msg)
{
    auto thread_id = std::this_thread::get_id();

    _mutex.lock_shared();

    auto it = _active_container_p->find(thread_id);
    auto* pqueue = it == _active_container_p->end() ? nullptr : &it->second;

    if (pqueue)
    {
        pqueue->push(std::move(msg));
        _mutex.unlock_shared();

        return;
    }

    _mutex.unlock_shared();

    _mutex.lock();
    (*_active_container_p)[thread_id].push(std::move(msg));
    _mutex.unlock();
}

void log_accumulator::release_logs_pre_init(size_t limit)
{
    size_t logs_count = 0;

    _mutex.lock();

    for (const auto& thread_logs : *_active_container_p)
        logs_count += thread_logs.second.size();

    if (logs_count > limit)
    {
        size_t logs_pop = logs_count - limit;

        LOG_WARN("Before initialization, " << logs_count << " logs were made. We delete the first " << logs_pop << " logs.");

        for (; logs_pop > 0; --logs_pop)
        {
            auto thread_id = get_oldest_log_thread_id(_active_container_p);
            SRV_ASSERT(thread_id, "The logs couldn't end");
            (*_active_container_p)[*thread_id].pop();
        }
    }

    _mutex.unlock();

    flush();
}

void log_accumulator::flush()
{
    _mutex.lock();

    if (_flush_active)
        return;

    _flush_active = true;

    std::swap(_active_container_p, _flush_container_p);

    _mutex.unlock();

    for (const auto& thread_logs : *_flush_container_p)
    {
        if (thread_logs.second.size() >= _limit_by_thread)
            LOG_ERROR("Thread " << thread_logs.second.front().context.thread_info.first << " spam logs");
    }

    while (true)
    {
        auto thread_id = get_oldest_log_thread_id(_flush_container_p);

        if (!thread_id)
            break;

        logger::instance().write((*_flush_container_p)[*thread_id].front());
        (*_flush_container_p)[*thread_id].pop();
    }

    _flush_active = false;
}

std::optional<std::thread::id> log_accumulator::get_oldest_log_thread_id(map_logs* p)
{
    std::thread::id thread_id(0);
    std::chrono::steady_clock::time_point oldest_time;

    for (const auto& thread_logs : *p)
    {
        if (thread_logs.second.empty())
            continue;

        if (thread_id == std::thread::id(0) || oldest_time > thread_logs.second.front().steady_time)
        {
            thread_id = thread_logs.first;
            oldest_time = thread_logs.second.front().steady_time;
        }
    }

    return thread_id != std::thread::id(0) ? std::make_optional(std::move(thread_id)) : std::nullopt;
}

} // namespace server_lib
