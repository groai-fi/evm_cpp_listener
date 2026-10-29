#pragma once
// Alert sender (plan 60 §5.3.1, slice 5-E2): POSTs one Alert from the rule engine to the n8n webhook.
//
// Delivery failure is visible but never fatal: a transport error or a non-2xx answer is retried `retries` times with
// jittered backoff; after that one ERROR is logged and `send_failures_total` is incremented. Nothing here touches
// /readyz or the pipeline. send() never blocks: each alert is delivered by its own detached coroutine on the executor.
//
// Credential protection: the webhook URL and the X-API-Key token are secrets. They are never logged, only the HTTP
// status or the (already redacted) transport error text. Constructing without them throws (fail fast).
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <spdlog/logger.h>

#include "ops/alerts.hpp"

namespace evml::ops {

struct AlertSenderOptions {
    std::string url;    // N8N_ALERT_WEBHOOK_URL value (secret)
    std::string token;  // N8N_ALERT_WEBHOOK_TOKEN value (secret)
    std::string service;
    std::string shard;
    std::string host;
    std::string user_agent{"evm-listener"};
    std::uint32_t retries{3};  // retries after the first attempt: 3 retries = at most 4 attempts
    std::chrono::milliseconds backoff_base{1000};
    std::chrono::milliseconds backoff_cap{30000};
    std::chrono::milliseconds http_timeout{10000};
    std::uint64_t seed{0};
    std::shared_ptr<spdlog::logger> logger;  // null = the global logger; tests inject a capturing one
};

/// @brief The JSON text posted to n8n: {severity, service, shard, host, title, message, ts, details}.
/// `ts` is the current UTC time as YYYY-MM-DDTHH:MM:SSZ.
std::string alert_payload(const AlertSenderOptions& opts, const Alert& alert);

class AlertSender {
public:
    /// @throws std::invalid_argument on an empty url or token
    AlertSender(boost::asio::any_io_executor executor, AlertSenderOptions opts);

    /// @brief Fire and forget. Call on the executor's thread (the host's io thread).
    void send(const Alert& alert);

    /// @brief Cancels in-flight webhook requests and retry timers on shutdown.
    void stop();

    /// @brief Alerts delivered with a 2xx answer.
    std::uint64_t sent_total() const;
    /// @brief Alerts given up on after all attempts (evml_alert_send_failures_total).
    std::uint64_t send_failures_total() const;

    struct State;  // shared with the detached delivery coroutines; internal

private:
    boost::asio::any_io_executor exec_;
    std::shared_ptr<State> state_;
};

}  // namespace evml::ops
