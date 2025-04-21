#include <server_lib/log_accumulator.h>

#include <server_lib/asserts.h>
#include <server_lib/logging_helper.h>

#include <iostream>

namespace server_lib {

log_accumulator::log_accumulator()
{
}

log_accumulator::~log_accumulator()
{
    if (_execute.load())
    {
        _execute.store(false);
        if (_thd.joinable())
            _thd.join();
    }

    flush();
}

void log_accumulator::init(size_t flush_period_ms, size_t limit_by_thread, size_t throttling_time_ms, size_t pre_init_logs_limit)
{
#if defined(_USE_LOG_ACCUMULATOR)

    _flush_period_ms.store(flush_period_ms);
    _limit_by_thread.store(limit_by_thread);
    _throttling_time_ms.store(throttling_time_ms);
    _execute.store(true);

    LOG_INFO("Logger Accumulator init. Flush period ms: " << flush_period_ms << ", limit logs by thread before "
                                                          << "throttling: " << limit_by_thread << ", throttling time in ms(for heavily spammy threads): "
                                                          << throttling_time_ms);

    if (_thd.joinable())
        return;

    release_logs_pre_init(pre_init_logs_limit);

    _thd = std::thread([this]() {
        while (_execute.load())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(_flush_period_ms));
            try
            {
                if (!logger::instance().is_force_flush_mode())
                {
                    flush();
                }
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
    if (!_execute)
    {
        logger::instance().write(msg);
        return;
    }

    if (logger::instance().is_force_flush_mode())
    {
        if (!_new_set_force_flush)
        {
            static std::mutex guard;
            const std::lock_guard<std::mutex> lock(guard);

            if (!_new_set_force_flush)
            {
                flush();
                _new_set_force_flush = true;
            }
        }

        logger::instance().write(msg);
        return;
    }

    _new_set_force_flush = false;

    add_log_msg(std::move(msg));
}

void log_accumulator::add_log_msg(logger::log_message&& msg)
{
    auto thread_id = std::this_thread::get_id();

    _mutex.lock_shared();

    auto it = _active_container.find(thread_id);
    if (it != _active_container.end())
    {
        auto& queue = it->second;
        queue.push(std::move(msg));
        size_t count_by_thread = queue.size();
        _mutex.unlock_shared();

        if (count_by_thread >= _limit_by_thread)
            std::this_thread::sleep_for(std::chrono::milliseconds(_throttling_time_ms));

        return;
    }

    _mutex.unlock_shared();

    _mutex.lock();
    _active_container[thread_id].push(std::move(msg));
    _mutex.unlock();
}

void log_accumulator::release_logs_pre_init(size_t limit)
{
    size_t logs_count = 0;

    _mutex.lock();

    for (const auto& thread_logs : _active_container)
        logs_count += thread_logs.second.size();

    if (logs_count > limit)
    {
        size_t logs_pop = logs_count - limit;

        LOG_WARN("Before initialization, " << logs_count << " logs were made. We delete the first " << logs_pop << " logs.");

        for (; logs_pop > 0; --logs_pop)
        {
            auto thread_ptr = get_oldest_log_thread(_active_container);
            SRV_ASSERT(thread_ptr, "The logs couldn't end");
            thread_ptr->pop();
        }
    }

    _mutex.unlock();

    flush();
}

void log_accumulator::flush()
{
    static std::mutex flush_guard;
    const std::lock_guard<std::mutex> lock(flush_guard);

    _mutex.lock();
    _active_container.swap(_flush_container);
    _mutex.unlock();

    auto it_thread_logs = _flush_container.begin();
    while (it_thread_logs != _flush_container.end())
    {
        const auto& thread_logs = it_thread_logs->second;

        if (thread_logs.empty())
        {
            it_thread_logs = _flush_container.erase(it_thread_logs);
            continue;
        }

        if (thread_logs.size() >= _limit_by_thread)
            LOG_ERROR("Thread " << thread_logs.front().context.thread_info.first << " spams logs");

        it_thread_logs++;
    }

    while (auto thread_ptr = get_oldest_log_thread(_flush_container))
    {
        logger::instance().write(thread_ptr->front());
        thread_ptr->pop();
    }
}

log_accumulator::logs_thread_ptr log_accumulator::get_oldest_log_thread(map_logs& p)
{
    logs_thread_ptr thread_ptr = nullptr;
    std::chrono::steady_clock::time_point oldest_time;

    for (auto& thread_logs : p)
    {
        if (thread_logs.second.empty())
            continue;

        if (thread_ptr == nullptr || oldest_time > thread_logs.second.front().steady_time)
        {
            thread_ptr = &thread_logs.second;
            oldest_time = thread_logs.second.front().steady_time;
        }
    }

    return thread_ptr;
}

} // namespace server_lib
