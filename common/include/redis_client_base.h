#pragma once

#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <mutex>
#include <chrono>
#include <atomic>
#include <functional>

using namespace std;

class RedisClientBase {
public:
    enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    };

    explicit RedisClientBase(const string& redis_url);
    virtual ~RedisClientBase();

    // Connection management
    bool connect();
    void disconnect();
    bool is_connected() const;

protected:
    // Helper methods
    static bool parse_redis_url(const string& url, string& host, int& port, string& username, string& password);
    void log_message(const string& level, const string& operation, const string& message);
    
    // Circuit breaker logic
    bool is_circuit_open();
    void record_failure();
    void record_success();

    // Command execution wrapper
    redisReply* execute_command(function<redisReply*(redisContext*)> command_fn);


    // Member variables
    string redis_url_;
    string username_;
    redisContext* context_;
    mutable mutex connection_mutex_;
    atomic<bool> connected_{false};

    // Circuit breaker state
    atomic<CircuitBreakerState> cb_state_{CircuitBreakerState::CLOSED};
    atomic<int> cb_failure_count_{0};
    atomic<long long> cb_last_failure_time_{0};

    const int cb_failure_threshold_ = 5; // failures to open circuit
    const chrono::seconds cb_open_duration_{30}; // duration to stay open
};
