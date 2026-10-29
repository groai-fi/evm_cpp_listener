#pragma once
// Dead-man's switch pinger (plan 60 §5.3.2, slice 5-E3): a GET to the healthchecks.io URL while this host is a healthy
// leader. When the pings stop, healthchecks.io pages, independent of Railway and n8n.
//
// Healthy = leader AND state is CatchingUp or Live AND sink not saturated AND (C_s advanced within the last 10 min OR
// G_s did not move either: a quiet chain is not a fault). The progress windows are tracked here from the facts the host
// passes on every tick(); the host only reports current values. A standby never pings.
//
// Failure (transport error or non-2xx) is logged and counted, never fatal. The URL is a credential: it is never logged
// (only the HTTP status or the redacted TransportError text). An empty URL disables pinging (WARN at construction,
// status() == "disabled" for /readyz).
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <spdlog/logger.h>

#include "ops/alerts.hpp"  // AlertClock

namespace evml::ops {

struct DeadmanOptions {
    std::string url;  // DEADMAN_PING_URL value (secret); empty = disabled
    std::string user_agent{"evm-listener"};
    std::chrono::milliseconds timeout{10000};
    std::chrono::seconds progress_window{600};  // "C_s advanced in the last 10 min"
    std::shared_ptr<spdlog::logger> logger;     // null = the global logger; tests inject a capturing one
};

struct DeadmanFacts {
    bool leader{false};
    bool state_ok{false};  // CatchingUp or Live
    bool sink_saturated{false};
    std::optional<std::uint64_t> c_s;  // checkpoint cursor
    std::optional<std::uint64_t> g_s;  // safe gate
    AlertClock::time_point now{};
};

class Deadman {
public:
    Deadman(boost::asio::any_io_executor executor, DeadmanOptions opts);

    /// @brief Feed the current facts; sends one ping if healthy (and none is still in flight).
    /// Call on the executor's thread at the ping cadence ([ops] deadman_ping_s).
    /// @return true if a ping was started
    bool tick(const DeadmanFacts& facts);

    /// @brief Cancels any in-flight dead-man ping on shutdown.
    void stop();

    bool enabled() const;
    /// @brief "enabled" or "disabled", for the /readyz JSON.
    const char* status() const;

    std::uint64_t pings_total() const;           // pings answered 2xx
    std::uint64_t ping_failures_total() const;   // evml_deadman_ping_failures_total

    struct State;  // shared with the detached ping coroutines; internal

private:
    bool healthy(const DeadmanFacts& f);

    boost::asio::any_io_executor exec_;
    std::shared_ptr<State> state_;
    // Progress trackers (io thread only).
    std::optional<std::uint64_t> last_c_, last_g_;
    AlertClock::time_point c_changed_{}, g_changed_{};
    bool tracking_{false};
};

}  // namespace evml::ops
