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
    using map_logs = std::map<std::thread::id, std::queue<logger::log_message>>;

public:
    virtual ~log_accumulator();

    // Reusable
    void init(size_t flush_period_ms, size_t limit_by_thread, size_t throttling_time_ms, size_t wait_flush,
              size_t pre_init_logs_limit);

    void put(logger::log_message&& msg);

protected:
    log_accumulator();

    friend class singleton<log_accumulator>;

private:
    void release_logs_pre_init(size_t limit);

    void add_log_msg(logger::log_message&& msg);
    void flush();

    std::optional<std::thread::id> get_oldest_log_thread_id(map_logs* p);

    std::vector<map_logs> _logs;

    map_logs* _active_container_p;
    map_logs* _flush_container_p;

    std::atomic<bool> _flush_active;
    std::atomic<bool> _new_set_force_flush;

    std::atomic<bool> _execute;
    std::thread _thd;
    std::shared_mutex _mutex;

    std::atomic<size_t> _flush_period_ms = 500;
    std::atomic<size_t> _limit_by_thread = 100000;
    std::atomic<size_t> _throttling_time_ms = 1;
    std::atomic<size_t> _wait_flush = 50;
};

} // namespace server_lib