#include <server_lib/log_accumulator.h>

#include <server_lib/asserts.h>
#include <server_lib/logging_helper.h>

#include <iostream>
#include <list>

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

    flush(false);
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

#if defined(SERVER_LIB_PLATFORM_LINUX)
        pthread_setname_np(pthread_self(), "logs-accum");
#endif

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
            if (_thd.get_id() == std::this_thread::get_id())
                return; // skip here calls from flush() to avoid deadlocks

            static std::mutex guard;
            const std::lock_guard<std::mutex> lock(guard);

            if (!_new_set_force_flush)
            {
                flush(false);
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
        queue.emplace_back(std::move(msg));
        size_t count_by_thread = queue.size();
        _mutex.unlock_shared();

        if (count_by_thread >= _limit_by_thread)
            std::this_thread::sleep_for(std::chrono::milliseconds(_throttling_time_ms));

        return;
    }

    _mutex.unlock_shared();

    _mutex.lock();
    _active_container[thread_id].emplace_back(std::move(msg));
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
            thread_ptr->pop_front();
        }
    }

    _mutex.unlock();

    flush(false);
}

void log_accumulator::flush(bool can_log)
{
    static std::mutex flush_guard;
    const std::lock_guard<std::mutex> lock(flush_guard);

    _mutex.lock();
    _active_container.swap(_flush_container);
    _mutex.unlock();

    auto flush_start_time = std::chrono::steady_clock::now();
    sorted_logs_threads sorted_threads;
    size_t total_num_messages = 0;

    auto it_thread_logs = _flush_container.begin();
    while (it_thread_logs != _flush_container.end())
    {
        auto& thread_logs = it_thread_logs->second;

        if (thread_logs.empty())
        {
            it_thread_logs = _flush_container.erase(it_thread_logs);
            continue;
        }

        total_num_messages += thread_logs.size();

        if (thread_logs.size() >= _limit_by_thread && can_log)
            LOG_ERROR("Thread " << thread_logs.front().context.thread_info.first << " spams logs: " << thread_logs.size());

        sorted_threads.emplace_back(std::make_pair(thread_logs.begin(), thread_logs.end()));
        it_thread_logs++;
    }

    auto messages_to_write = total_num_messages;

    while (!sorted_threads.empty())
    {
        // sort threads by oldest message time
        sort_logs_threads(sorted_threads);

        auto& cur_thread = sorted_threads.back();
        auto* next_thread = (sorted_threads.size() > 1) ? &sorted_threads[sorted_threads.size() - 2] : nullptr;
        auto next_thread_time = next_thread ? next_thread->first->steady_time : flush_start_time;

        bool written_any = false;

        while (cur_thread.first != cur_thread.second && cur_thread.first->steady_time <= next_thread_time)
        {
            logger::instance().write(*cur_thread.first);
            cur_thread.first++;
            messages_to_write--;
            written_any = true;
        }

        // we know that next thread first message will be written at first - so do it here
        if (next_thread)
        {
            logger::instance().write(*next_thread->first);
            next_thread->first++;
            messages_to_write--;
            written_any = true;
        }

        // remove empty thread(s)
        auto it = sorted_threads.end() - (next_thread ? 2 : 1);
        while (it != sorted_threads.end())
        {
            if (it->first == it->second) // this thread is finished - remove it from the list
                it = sorted_threads.erase(it);
            else
                it++;
        }

        if (!written_any)
        {
            if (can_log)
                LOG_ERROR("Logs flush is stuck! Breaking.");

            break;
        }
    }

    // cleanup all flushed threads
    for (auto& it_thread : _flush_container)
    {
        it_thread.second.clear();
    }

    if (can_log)
    {
        if (messages_to_write != 0)
            LOG_ERROR("log_accumulator::flush - not all messages were wtitten: " << messages_to_write);

        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = (size_t)std::chrono::duration_cast<std::chrono::milliseconds>(now - flush_start_time).count();
        LOG_TRACE("log_accumulator::flush took " << elapsed_ms << " ms (" << _flush_container.size() << " threads, " << total_num_messages << " rows)");
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

void log_accumulator::sort_logs_threads(sorted_logs_threads& threads)
{
    // desc by message steady_time
    std::sort(threads.begin(), threads.end(),
              [](const sorted_logs_thread& thread1, const sorted_logs_thread& thread2) {
                  return thread1.first->steady_time > thread2.first->steady_time;
              });
}

} // namespace server_lib
