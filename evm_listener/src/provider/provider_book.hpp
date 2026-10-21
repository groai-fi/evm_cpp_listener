#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rpc/classify.hpp"

namespace evml::provider {

using TimePoint = std::chrono::steady_clock::time_point;
using std::chrono::seconds;

/// @brief Health states for RPC endpoints.
enum class State {
    Up,        // usable
    Down,      // repeated transient failures or voluntarily demoted: must pass a probe to come back
    Cooldown,  // monthly quota exhausted: re-probed after quota_cooldown
    Disabled,  // credentials rejected: re-probed after auth_disable
    Fatal,     // configuration error (wrong chain): never used again in this process
};
const char* to_string(State s);

/// @brief Thresholds and cooldowns governing provider health state transitions and failover hysteresis.
struct Policy {
    unsigned failover_after{3};            // consecutive session/request failures before Up -> Down
    seconds quota_cooldown{3600};          // QuotaExhausted
    seconds auth_disable{600};             // AuthFatal
    seconds probe_interval{30};            // re-probe cadence for Down providers
    seconds default_rate_limit_wait{10};   // RateLimited without a Retry-After hint
    seconds prefer_primary_after{600};     // a better-ranked provider must be healthy this long to take over again
    seconds min_voluntary_gap{60};         // spacing between switch-backs / stale demotions / forced failovers
};

/// @brief Per-provider health record tracking consecutive errors, rate limit backoff, and probe timers.
struct ProviderState {
    State state{State::Up};
    unsigned consecutive_failures{0};
    std::optional<TimePoint> healthy_since;  // set on success/probe-ok, cleared on any failure
    TimePoint next_probe_at{};               // meaningful for Down / Cooldown / Disabled
    TimePoint not_before{};                  // RateLimited: do not send traffic before this
};

// A state change worth an alert/log line (one per incident, never per retry).
/// @brief Recorded health state transition for observability and alerting.
struct Transition {
    std::string provider;
    State from;
    State to;
};

// Pure failover policy for the two provider chains (WS order, getLogs order). No I/O, no clock: every call
// takes `now`, so tests drive it with a fake time. Single-threaded.
//
// Rules (plan 2.3 / 2.5):
//  - Retry / RateLimited failures count; at `failover_after` consecutive ones the provider goes Down.
//  - QuotaExhausted -> Cooldown immediately; AuthFatal -> Disabled immediately; ConfigFatal -> Fatal forever.
//  - RangeTooLarge is NOT a provider failure and is ignored here.
//  - A non-Up provider returns to Up only through a successful probe/traffic after its probe time.
//  - The active WS provider is kept while it is Up (even when rate-limited: the session owner backs off).
//    Forced failovers (active not Up) are immediate. Voluntary switches (switch-back to a better provider,
//    stale demotion) are spaced by `min_voluntary_gap`, and switch-back additionally requires the better provider
//    to have been healthy for `prefer_primary_after`.
/// @brief Deterministic failover book tracking provider health, rate limits, and dual-provider cross-checking.
/// Invariants & Rules:
/// - Sticky Primary: Current WebSocket provider is retained while Up; primary switchback requires 10 min sustained health.
/// - Independent Cross-Check: `pick_getlogs` prefers a different provider than `active_ws` to cross-validate chain state.
/// - Anti-Flap: Minimum 60-second gap enforced between voluntary switches and stale demotions.
class ProviderBook {
public:
    // Throws std::invalid_argument for a duplicate name within one list or two empty lists.
    /// @brief Initializes provider book with priority order for WebSocket and HTTP getLogs endpoints.
    ProviderBook(std::vector<std::string> ws_order, std::vector<std::string> getlogs_order, Policy policy = {});

    // ---- events (unknown provider names throw std::out_of_range) ----
    /// @brief Records request failure and updates state (Down on consecutive retries, Cooldown on quota, Disabled on auth).
    std::optional<Transition> on_failure(std::string_view name, rpc::FailKind kind,
                                         std::optional<seconds> retry_after, TimePoint now);
    /// @brief Records successful call, resetting failure counts and promoting to Up.
    std::optional<Transition> on_success(std::string_view name, TimePoint now);
    /// @brief Records health probe outcome, transitioning Down providers back to Up if successful.
    std::optional<Transition> on_probe_result(std::string_view name, bool ok, TimePoint now);
    // Stale/lagging provider demoted although it raised no error. nullopt = refused (inside min_voluntary_gap)
    // or the provider is not Up.
    /// @brief Demotes lagging provider voluntarily; gated by anti-flap cooldown window.
    std::optional<Transition> demote_voluntary(std::string_view name, TimePoint now);

    // ---- queries ----
    /// @brief Checks whether probe interval has elapsed for a Down/Cooldown/Disabled provider.
    bool probe_due(std::string_view name, TimePoint now) const;
    /// @brief Returns true if provider is Up and not currently subject to rate-limit backoff.
    bool usable(std::string_view name, TimePoint now) const;  // Up and not rate-limited right now
    /// @brief Returns current state record of named provider.
    const ProviderState& state(std::string_view name) const;

    struct WsDecision {
        enum class Reason { Keep, Initial, Forced, SwitchBack, NoneAvailable };
        std::optional<std::string> provider;  // the provider that should be active (nullopt = none)
        bool changed{false};
        Reason reason{Reason::Keep};
    };
    // Stateful: remembers the active WS provider. Call after any event and periodically.
    /// @brief Evaluates active WebSocket provider selection, applying sticky retention and 10-minute switchback hysteresis.
    WsDecision select_ws(TimePoint now);
    /// @brief Returns currently active WebSocket provider name, or nullopt if none assigned.
    const std::optional<std::string>& active_ws() const { return active_ws_; }

    // First usable getLogs provider that is NOT the active WS provider (independent cross-check); falls back to
    // the active WS provider only when nothing else is usable. nullopt = none usable.
    /// @brief Selects optimal HTTP provider for getLogs; prioritizes non-active WS provider for dual-node cross-check.
    std::optional<std::string> pick_getlogs(TimePoint now) const;
    // Same, skipping providers in `exclude` (those already tried for the current request).
    /// @brief Selects getLogs provider excluding already tried endpoints during failover retry loops.
    std::optional<std::string> pick_getlogs(TimePoint now, const std::unordered_set<std::string>& exclude) const;

private:
    ProviderState& at(std::string_view name);
    const ProviderState& at(std::string_view name) const;
    static std::optional<Transition> set_state(ProviderState& ps, std::string_view name, State to);
    bool voluntary_allowed(TimePoint now) const;

    std::vector<std::string> ws_order_;
    std::vector<std::string> getlogs_order_;
    Policy policy_;
    std::unordered_map<std::string, ProviderState> providers_;
    std::optional<std::string> active_ws_;
    std::optional<TimePoint> last_switch_at_;
};

}  // namespace evml::provider
