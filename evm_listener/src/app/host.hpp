#pragma once
// The listener host (plan 60 §5.H, slice 5-D1): builds sink + pool + gate bridge + sweeper + driver, runs the tick
// loop on the calling thread (the "io thread"), serves the ops endpoints, and runs the process watchdog.
// D1 supports `[lease] enabled = false` only (single host); the lease and standby loop arrive in 5-D2.
#include <atomic>
#include <chrono>
#include <functional>
#include <optional>

#include "core/config.hpp"
#include "pipeline/pipeline_driver.hpp"

namespace evml::app {

struct HostOptions {
    /// Shutdown request (SIGTERM/SIGINT handler or a test). Required. Read on every beat, so a stop is noticed within
    /// `beat_period`.
    const std::atomic<bool>* stop{nullptr};
    /// How often the io thread beats the watchdog, publishes state and polls `stop` / the sink. It only has to prove the
    /// io loop is running, so it is independent of how long one pipeline tick takes.
    std::chrono::milliseconds beat_period{1000};
    /// Called on the io thread every beat once the pipeline exists (tests watch for Live).
    std::function<void(const pipeline::DriverStatus&)> on_status;
    /// Test hook (`--test-freeze-io-after`, only reachable from the CLI when built with EVML_TEST_HOOKS): blocks the io
    /// thread after this long, so the watchdog must abort the process.
    std::optional<std::chrono::seconds> test_freeze_io_after;
};

/// @brief Runs the listener until `*opt.stop` is set or the pipeline ends. Returns the process exit code: 0 graceful
/// stop, 1 runtime failure (sink Failed, Redis connection error), 2 operator action required (driver Failed, unusable
/// config such as `[lease] enabled = true` before 5-D2, Redis version/eviction policy).
/// @throws core::ConfigError if the Redis URL env var is missing; boost::system::system_error if the ops port cannot be bound.
int run_host(const core::Config& cfg, const HostOptions& opt);

/// @brief Production entry: initialises logging, installs SIGTERM/SIGINT handlers, then run_host().
int run_listener(const core::Config& cfg, std::optional<std::chrono::seconds> test_freeze_io_after);

}  // namespace evml::app
