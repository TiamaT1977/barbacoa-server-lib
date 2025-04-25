#pragma once

#include <chrono>
#include <memory>
#include <atomic>

#include <server_lib/types.h>

namespace server_lib {

template <typename EventLoop>
class timer
{
    DECLARE_PTR(id)
    class id
    {
    public:
        std::atomic<int> value = 0;
    };

public:
    timer(EventLoop& el)
        : _el(el)
        , _current_timer_id(std::make_shared<id>())
    {
    }

    ~timer()
    {
        stop();
    }

    template <typename DurationType, typename Callback>
    bool start(DurationType&& duration, Callback&& callback)
    {
        int timer_id = ++_current_timer_id->value;
        auto id = _current_timer_id;

        if (timer_id <= 0)
        {
            --_current_timer_id->value;
            return false;
        }

        _el.start_timer(std::forward<DurationType>(duration), [=]() {
            if (id->value == timer_id)
                callback();
        });

        return true;
    }


    virtual void stop()
    {
        _current_timer_id->value = -1;
    }

private:
    EventLoop& _el;
    id_ptr _current_timer_id;
};


template <typename EventLoop>
class periodical_timer
{
public:
    periodical_timer(EventLoop& el)
        : _timer(el)
    {
    }

    ~periodical_timer()
    {
        stop();
    }

    template <typename DurationType, typename Callback>
    bool start(DurationType&& duration, Callback&& callback)
    {
        return _timer.start(std::forward<DurationType>(duration), [=]() {
            if (this->start(duration, callback))
                callback();
        });
    }

    void stop()
    {
        _timer.stop();
    }

private:
    timer<EventLoop> _timer;
};


} // namespace server_lib
