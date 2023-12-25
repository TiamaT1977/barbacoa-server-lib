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
    void flush();

    std::pair<std::thread::id /*thread id*/, uint64_t /*time in ms*/> get_oldest_log();

    std::vector<map_logs> _logs;

    std::atomic<std::uint16_t> _act_index;

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