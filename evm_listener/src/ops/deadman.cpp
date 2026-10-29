#include "ops/deadman.hpp"

#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include "core/log.hpp"
#include "ops/http_client.hpp"

namespace evml::ops {

namespace net = boost::asio;
namespace log = core::log;

struct Deadman::State {
    State(net::any_io_executor ex, DeadmanOptions o)
        : opts(std::move(o)),
          client(std::move(ex), HttpClientOptions{.user_agent = opts.user_agent, .timeout = opts.timeout}) {}

    void write(spdlog::level::level_enum lvl, std::string_view msg, std::initializer_list<log::Field> fields) const {
        if (opts.logger)
            log::emit(*opts.logger, lvl, "deadman", msg, fields);
        else if (lvl == spdlog::level::err)
            log::error("deadman", msg, fields);
        else
            log::warn("deadman", msg, fields);
    }

    const DeadmanOptions opts;
    HttpClient client;
    bool stopped{false};
    std::atomic<bool> in_flight{false};
    std::atomic<std::uint64_t> pings{0};
    std::atomic<std::uint64_t> failures{0};
};

namespace {

net::awaitable<void> ping(std::shared_ptr<Deadman::State> st) {
    std::string error;
    try {
        const auto res = co_await st->client.get(st->opts.url);
        if (res.status >= 200 && res.status < 300) {
            ++st->pings;
            st->in_flight = false;
            co_return;
        }
        error = "http status " + std::to_string(res.status);
    } catch (const std::exception& e) {
        error = e.what();  // TransportError text carries the redacted URL only
    }
    if (!st->stopped) {
        ++st->failures;
        st->write(spdlog::level::warn, "dead-man ping failed", {{"error", error}});
    }
    st->in_flight = false;
}

}  // namespace

Deadman::Deadman(net::any_io_executor executor, DeadmanOptions opts) : exec_(executor) {
    const bool disabled = opts.url.empty();
    state_ = std::make_shared<State>(std::move(executor), std::move(opts));
    if (disabled)
        state_->write(spdlog::level::warn, "DEADMAN_PING_URL is empty: dead-man pings are disabled", {});
}

bool Deadman::enabled() const { return !state_->opts.url.empty(); }

const char* Deadman::status() const { return enabled() ? "enabled" : "disabled"; }

bool Deadman::healthy(const DeadmanFacts& f) {
    if (!f.leader) {
        tracking_ = false;  // a standby never pings; restart the windows when it becomes leader
        return false;
    }
    if (!tracking_) {
        last_c_ = f.c_s;
        last_g_ = f.g_s;
        c_changed_ = g_changed_ = f.now;
        tracking_ = true;
    } else {
        if (f.c_s != last_c_) {
            last_c_ = f.c_s;
            c_changed_ = f.now;
        }
        if (f.g_s != last_g_) {
            last_g_ = f.g_s;
            g_changed_ = f.now;
        }
    }
    const auto window = state_->opts.progress_window;
    const bool c_stale = f.now - c_changed_ > window;
    const bool g_moved = f.now - g_changed_ <= window;
    return f.state_ok && !f.sink_saturated && !(c_stale && g_moved);
}

bool Deadman::tick(const DeadmanFacts& facts) {
    const bool ok = healthy(facts);
    if (!enabled() || !ok || state_->stopped) return false;
    if (state_->in_flight.exchange(true)) return false;  // previous ping still running (10 s timeout bounds it)
    net::co_spawn(exec_, ping(state_), net::detached);
    return true;
}

void Deadman::stop() {
    state_->stopped = true;
    state_->client.cancel();
}

std::uint64_t Deadman::pings_total() const { return state_->pings.load(); }
std::uint64_t Deadman::ping_failures_total() const { return state_->failures.load(); }

}  // namespace evml::ops
