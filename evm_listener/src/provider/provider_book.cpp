#include "provider/provider_book.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::provider {

const char* to_string(State s) {
    switch (s) {
        case State::Up: return "up";
        case State::Down: return "down";
        case State::Cooldown: return "cooldown";
        case State::Disabled: return "disabled";
        case State::Fatal: return "fatal";
    }
    return "unknown";
}

namespace {
bool has_duplicates(const std::vector<std::string>& v) {
    auto c = v;
    std::sort(c.begin(), c.end());
    return std::adjacent_find(c.begin(), c.end()) != c.end();
}
}  // namespace

ProviderBook::ProviderBook(std::vector<std::string> ws_order, std::vector<std::string> getlogs_order, Policy policy)
    : ws_order_(std::move(ws_order)), getlogs_order_(std::move(getlogs_order)), policy_(policy) {
    if (ws_order_.empty() && getlogs_order_.empty()) throw std::invalid_argument("ProviderBook: no providers");
    if (has_duplicates(ws_order_)) throw std::invalid_argument("ProviderBook: duplicate name in ws_order");
    if (has_duplicates(getlogs_order_)) throw std::invalid_argument("ProviderBook: duplicate name in getlogs_order");
    for (const auto& n : ws_order_) providers_.emplace(n, ProviderState{});
    for (const auto& n : getlogs_order_) providers_.emplace(n, ProviderState{});
}

ProviderState& ProviderBook::at(std::string_view name) {
    const auto it = providers_.find(std::string(name));
    if (it == providers_.end()) throw std::out_of_range("ProviderBook: unknown provider '" + std::string(name) + "'");
    return it->second;
}
const ProviderState& ProviderBook::at(std::string_view name) const {
    const auto it = providers_.find(std::string(name));
    if (it == providers_.end()) throw std::out_of_range("ProviderBook: unknown provider '" + std::string(name) + "'");
    return it->second;
}
const ProviderState& ProviderBook::state(std::string_view name) const { return at(name); }

std::optional<Transition> ProviderBook::set_state(ProviderState& ps, std::string_view name, State to) {
    if (ps.state == to) return std::nullopt;
    const State from = ps.state;
    ps.state = to;
    return Transition{std::string(name), from, to};
}

std::optional<Transition> ProviderBook::on_failure(std::string_view name, rpc::FailKind kind,
                                                   std::optional<seconds> retry_after, TimePoint now) {
    auto& ps = at(name);
    if (ps.state == State::Fatal) return std::nullopt;
    if (kind == rpc::FailKind::RangeTooLarge) return std::nullopt;  // the caller shrinks its range; provider is fine

    ps.healthy_since.reset();
    switch (kind) {
        case rpc::FailKind::ConfigFatal:
            return set_state(ps, name, State::Fatal);
        case rpc::FailKind::QuotaExhausted:
            ps.next_probe_at = now + policy_.quota_cooldown;
            return set_state(ps, name, State::Cooldown);
        case rpc::FailKind::AuthFatal:
            ps.next_probe_at = now + policy_.auth_disable;
            return set_state(ps, name, State::Disabled);
        case rpc::FailKind::RateLimited:
            ps.not_before = std::max(ps.not_before, now + retry_after.value_or(policy_.default_rate_limit_wait));
            [[fallthrough]];
        case rpc::FailKind::Retry:
            ++ps.consecutive_failures;
            if (ps.state == State::Up && ps.consecutive_failures >= policy_.failover_after) {
                ps.next_probe_at = now + policy_.probe_interval;
                return set_state(ps, name, State::Down);
            }
            return std::nullopt;
        case rpc::FailKind::RangeTooLarge:
            break;  // handled above
    }
    return std::nullopt;
}

std::optional<Transition> ProviderBook::on_success(std::string_view name, TimePoint now) {
    auto& ps = at(name);
    if (ps.state == State::Fatal) return std::nullopt;
    ps.consecutive_failures = 0;
    if (!ps.healthy_since) ps.healthy_since = now;
    return set_state(ps, name, State::Up);
}

std::optional<Transition> ProviderBook::on_probe_result(std::string_view name, bool ok, TimePoint now) {
    auto& ps = at(name);
    if (ps.state == State::Fatal) return std::nullopt;
    if (ok) return on_success(name, now);
    if (ps.state == State::Up) return on_failure(name, rpc::FailKind::Retry, std::nullopt, now);
    ps.healthy_since.reset();
    switch (ps.state) {
        case State::Cooldown: ps.next_probe_at = now + policy_.quota_cooldown; break;
        case State::Disabled: ps.next_probe_at = now + policy_.auth_disable; break;
        default: ps.next_probe_at = now + policy_.probe_interval; break;
    }
    return std::nullopt;
}

bool ProviderBook::voluntary_allowed(TimePoint now) const {
    return !last_switch_at_ || now - *last_switch_at_ >= policy_.min_voluntary_gap;
}

std::optional<Transition> ProviderBook::demote_voluntary(std::string_view name, TimePoint now) {
    auto& ps = at(name);
    if (ps.state != State::Up || !voluntary_allowed(now)) return std::nullopt;
    ps.healthy_since.reset();
    ps.next_probe_at = now + policy_.probe_interval;
    last_switch_at_ = now;
    return set_state(ps, name, State::Down);
}

bool ProviderBook::probe_due(std::string_view name, TimePoint now) const {
    const auto& ps = at(name);
    const bool probeable = ps.state == State::Down || ps.state == State::Cooldown || ps.state == State::Disabled;
    return probeable && now >= ps.next_probe_at;
}

bool ProviderBook::usable(std::string_view name, TimePoint now) const {
    const auto& ps = at(name);
    return ps.state == State::Up && now >= ps.not_before;
}

ProviderBook::WsDecision ProviderBook::select_ws(TimePoint now) {
    using Reason = WsDecision::Reason;
    const auto first_usable = [&]() -> std::optional<std::string> {
        for (const auto& n : ws_order_) {
            if (usable(n, now)) return n;
        }
        return std::nullopt;
    };

    if (!active_ws_) {
        if (auto p = first_usable()) {
            active_ws_ = p;
            return {p, true, Reason::Initial};
        }
        return {std::nullopt, false, Reason::NoneAvailable};
    }

    // The active provider is kept while Up, even if momentarily rate-limited (its session backs off itself).
    if (at(*active_ws_).state != State::Up) {
        auto p = first_usable();
        active_ws_ = p;
        last_switch_at_ = now;
        return {p, true, p ? Reason::Forced : Reason::NoneAvailable};
    }

    // Switch back to a better-ranked provider only after it proved stable, and not in a flap window.
    if (voluntary_allowed(now)) {
        for (const auto& n : ws_order_) {
            if (n == *active_ws_) break;  // only providers ranked above the active one
            const auto& ps = at(n);
            if (usable(n, now) && ps.healthy_since && now - *ps.healthy_since >= policy_.prefer_primary_after) {
                active_ws_ = n;
                last_switch_at_ = now;
                return {n, true, Reason::SwitchBack};
            }
        }
    }
    return {active_ws_, false, Reason::Keep};
}

std::optional<std::string> ProviderBook::pick_getlogs(TimePoint now) const {
    for (const auto& n : getlogs_order_) {
        if (active_ws_ && n == *active_ws_) continue;
        if (usable(n, now)) return n;
    }
    if (active_ws_ && std::find(getlogs_order_.begin(), getlogs_order_.end(), *active_ws_) != getlogs_order_.end() &&
        usable(*active_ws_, now)) {
        return active_ws_;
    }
    return std::nullopt;
}

}  // namespace evml::provider
