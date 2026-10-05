#include "redis_client_base.h"
#include <iostream>
#include <cstring>
#include <thread>

RedisClientBase::RedisClientBase(const string& redis_url)
    : redis_url_(redis_url), context_(nullptr) {}

RedisClientBase::~RedisClientBase() {
    disconnect();
}

bool RedisClientBase::parse_redis_url(const string& url, string& host, int& port, string& username,
                                  string& password) {
    string cleaned_url = url;
    const string redis_prefix = "redis://";
    if (cleaned_url.length() >= redis_prefix.length() &&
        cleaned_url.compare(0, redis_prefix.length(), redis_prefix) == 0) {
        cleaned_url = cleaned_url.substr(redis_prefix.length());
    }

    size_t at_pos = cleaned_url.rfind('@');
    if (at_pos != string::npos) {
        string auth_part = cleaned_url.substr(0, at_pos);
        cleaned_url = cleaned_url.substr(at_pos + 1);

        size_t colon_pos = auth_part.find(':');
        if (colon_pos != string::npos) {
            username = auth_part.substr(0, colon_pos);
            password = auth_part.substr(colon_pos + 1);
        } else {
            password = auth_part;
        }
    }

    size_t colon_pos = cleaned_url.find(':');
    if (colon_pos != string::npos) {
        host = cleaned_url.substr(0, colon_pos);
        try {
            port = stoi(cleaned_url.substr(colon_pos + 1));
        } catch (const exception& e) {
            cerr << "[RedisClientBase] Invalid port number: " << cleaned_url.substr(colon_pos + 1) << endl;
            return false;
        }
    } else {
        host = cleaned_url;
        port = 6379;
    }

    return true;
}

void RedisClientBase::log_message(const string& level, const string& operation, const string& message) {
    cout << "[" << level << "][RedisClientBase][" << operation << "] " << message << endl;
}

bool RedisClientBase::connect() {
    lock_guard<mutex> lock(connection_mutex_);

    if (connected_ && context_) {
        return true;
    }

    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }

    string host, password;
    int port;

    if (!parse_redis_url(redis_url_, host, port, username_, password)) {
        return false;
    }

    struct timeval timeout = {2, 0}; // 2 seconds

    log_message("INFO", "connect", "Connecting to Redis: " + host + ":" + to_string(port));
    context_ = redisConnectWithTimeout(host.c_str(), port, timeout);

    if (!context_ || context_->err) {
        string error_msg = context_ ? context_->errstr : "Failed to allocate Redis context";
        log_message("ERROR", "connect", "Connection failed: " + error_msg);
        if (context_) {
            redisFree(context_);
            context_ = nullptr;
        }
        connected_ = false;
        return false;
    }

    redisSetTimeout(context_, timeout);

    if (!password.empty()) {
        redisReply* reply;
        if (!username_.empty()) {
            reply = (redisReply*)redisCommand(context_, "AUTH %s %s", username_.c_str(), password.c_str());
        } else {
            reply = (redisReply*)redisCommand(context_, "AUTH %s", password.c_str());
        }

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            string error_msg = reply ? reply->str : "Authentication failed";
            log_message("ERROR", "connect", "Authentication failed: " + error_msg);
            if (reply) freeReplyObject(reply);
            redisFree(context_);
            context_ = nullptr;
            connected_ = false;
            return false;
        }
        freeReplyObject(reply);
    }

    redisReply* ping_reply = (redisReply*)redisCommand(context_, "PING");
    if (!ping_reply || ping_reply->type != REDIS_REPLY_STATUS || strcmp(ping_reply->str, "PONG") != 0) {
        log_message("ERROR", "connect", "PING test failed");
        if (ping_reply) freeReplyObject(ping_reply);
        redisFree(context_);
        context_ = nullptr;
        connected_ = false;
        return false;
    }

    freeReplyObject(ping_reply);
    connected_ = true;
    record_success(); // Reset circuit breaker on successful connection
    log_message("INFO", "connect", "Connected to Redis at " + host + ":" + to_string(port));
    return true;
}

void RedisClientBase::disconnect() {
    lock_guard<mutex> lock(connection_mutex_);
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
    connected_ = false;
}

bool RedisClientBase::is_connected() const {
    return connected_ && context_;
}

// --- Circuit Breaker Implementation ---

bool RedisClientBase::is_circuit_open() {
    if (cb_state_.load() == CircuitBreakerState::OPEN) {
        auto now = chrono::steady_clock::now().time_since_epoch().count();
        if ((now - cb_last_failure_time_.load()) > cb_open_duration_.count() * 1000000000LL) {
            cb_state_.store(CircuitBreakerState::HALF_OPEN);
            return false; // Allow one trial request
        }
        return true;
    }
    return false;
}

void RedisClientBase::record_failure() {
    cb_failure_count_++;
    if (cb_state_.load() == CircuitBreakerState::HALF_OPEN) {
        cb_state_.store(CircuitBreakerState::OPEN);
        cb_last_failure_time_.store(chrono::steady_clock::now().time_since_epoch().count());
        log_message("WARN", "circuit_breaker", "Circuit breaker re-opened after failure in HALF_OPEN state.");
    } else if (cb_failure_count_ >= cb_failure_threshold_) {
        cb_state_.store(CircuitBreakerState::OPEN);
        cb_last_failure_time_.store(chrono::steady_clock::now().time_since_epoch().count());
        log_message("WARN", "circuit_breaker", "Circuit breaker opened after " + to_string(cb_failure_count_) + " failures.");
    }
}

void RedisClientBase::record_success() {
    if (cb_state_.load() == CircuitBreakerState::HALF_OPEN) {
        log_message("INFO", "circuit_breaker", "Circuit breaker closed after success in HALF_OPEN state.");
    }
    cb_failure_count_ = 0;
    cb_state_.store(CircuitBreakerState::CLOSED);
}

redisReply* RedisClientBase::execute_command(function<redisReply*(redisContext*)> command_fn) {
    if (is_circuit_open()) {
        log_message("WARN", "execute_command", "Circuit breaker is open. Command not executed.");
        return nullptr;
    }

    redisReply* reply = nullptr;
    int max_retries = 3;

    for (int i = 0; i < max_retries; ++i) {
        if (!is_connected()) {
            log_message("INFO", "execute_command", "Not connected. Attempting to connect...");
            if (!connect()) {
                if (i < max_retries - 1) {
                    this_thread::sleep_for(chrono::seconds(1));
                    continue;
                }
                log_message("ERROR", "execute_command", "Failed to connect after retries.");
                record_failure();
                return nullptr;
            }
        }

        {
            lock_guard<mutex> lock(connection_mutex_);
            if (context_) {
                reply = command_fn(context_);
            }
        }
        
        if (reply) {
            if (reply->type != REDIS_REPLY_ERROR) {
                record_success();
                return reply;
            } else {
                 log_message("ERROR", "execute_command", "Redis error: " + string(reply->str));
                 // Do not retry on command-level errors
                 record_failure();
                 return reply;
            }
        }

        // Handle connection-level errors
        if (context_ && context_->err) {
            log_message("ERROR", "execute_command", "Connection error: " + string(context_->errstr));
            disconnect();
        } else {
            log_message("WARN", "execute_command", "No reply received.");
        }

        if (i < max_retries - 1) {
            log_message("INFO", "execute_command", "Retrying command...");
            this_thread::sleep_for(chrono::seconds(1));
        }
    }

    log_message("ERROR", "execute_command", "Command failed after multiple retries.");
    record_failure();
    return nullptr;
}
