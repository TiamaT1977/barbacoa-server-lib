#pragma once

#include <map>
#include <shared_mutex>
#include <thread>

#include <queue>

#include <server_lib/logger.h>
#include <server_lib/singleton.h>

namespace server_lib {

class log_accumulator : public singleton<log_accumulator>
{
    using logs_thread = std::queue<logger::log_message>;
    using logs_thread_ptr = logs_thread*;
    using map_logs = std::map<std::thread::id, logs_thread>;

public:
    virtual ~log_accumulator();

    // Reusable
    void init(size_t flush_period_ms, size_t limit_by_thread, size_t throttling_time_ms, size_t pre_init_logs_limit);

    void put(logger::log_message&& msg);

protected:
    log_accumulator();

    friend class singleton<log_accumulator>;

private:
    void release_logs_pre_init(size_t limit);

    void add_log_msg(logger::log_message&& msg);
    void flush();

    logs_thread_ptr get_oldest_log_thread(map_logs& p);

    map_logs _active_container;
    map_logs _flush_container;

    std::atomic<bool> _new_set_force_flush = false;

    std::atomic<bool> _execute = false;
    std::thread _thd;
    std::shared_mutex _mutex;

    std::atomic<size_t> _flush_period_ms = 500;
    std::atomic<size_t> _limit_by_thread = 100000;
    std::atomic<size_t> _throttling_time_ms = 1;
};

} // namespace server_lib
