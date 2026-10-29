#include "ops/alert_sender.hpp"

#include <algorithm>
#include <ctime>
#include <stdexcept>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>

#include "core/log.hpp"
#include "ops/http_client.hpp"
#include "rpc/backoff.hpp"

namespace evml::ops {

namespace net = boost::asio;
namespace json = boost::json;
namespace log = core::log;

namespace {

std::string utc_now_iso() {
    const std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

}  // namespace

std::string alert_payload(const AlertSenderOptions& opts, const Alert& alert) {
    json::object o;
    o["severity"] = to_string(alert.severity);
    o["service"] = opts.service;
    o["shard"] = opts.shard;
    o["host"] = opts.host;
    o["title"] = alert.title;
    o["message"] = alert.message;
    o["ts"] = utc_now_iso();
    o["details"] = alert.details;
    return json::serialize(o);
}

struct AlertSender::State {
    State(net::any_io_executor ex, AlertSenderOptions o)
        : opts(std::move(o)),
          client(std::move(ex), HttpClientOptions{.user_agent = opts.user_agent, .timeout = opts.http_timeout}) {}

    void write(spdlog::level::level_enum lvl, std::string_view msg, std::initializer_list<log::Field> fields) const {
        if (opts.logger)
            log::emit(*opts.logger, lvl, "alert", msg, fields);
        else if (lvl == spdlog::level::err)
            log::error("alert", msg, fields);
        else
            log::warn("alert", msg, fields);
    }

    const AlertSenderOptions opts;
    HttpClient client;
    bool stopped{false};
    std::vector<net::steady_timer*> retry_timers;
    std::atomic<std::uint64_t> sent{0};
    std::atomic<std::uint64_t> failures{0};
    std::atomic<std::uint64_t> seq{0};
};

namespace {

net::awaitable<void> deliver(std::shared_ptr<AlertSender::State> st, std::string title, std::string body,
                             std::uint64_t seq) {
    rpc::Backoff backoff(st->opts.seed + seq, st->opts.backoff_base, st->opts.backoff_cap);
    const HttpClient::Headers headers{{"X-API-Key", st->opts.token}};
    const std::uint32_t attempts = st->opts.retries + 1;
    std::string last_error;
    for (std::uint32_t i = 1; i <= attempts; ++i) {
        if (st->stopped) co_return;
        try {
            const auto res = co_await st->client.post_json(st->opts.url, body, headers);
            if (res.status >= 200 && res.status < 300) {
                ++st->sent;
                co_return;
            }
            last_error = "http status " + std::to_string(res.status);
        } catch (const std::exception& e) {
            last_error = e.what();  // TransportError text carries the redacted URL only
        }
        if (st->stopped) co_return;
        const auto n = std::to_string(i);
        if (i < attempts) {
            st->write(spdlog::level::warn, "alert delivery attempt failed, will retry",
                      {{"title", title}, {"attempt", n}, {"error", last_error}});
            net::steady_timer t(co_await net::this_coro::executor);
            st->retry_timers.push_back(&t);
            struct TimerGuard {
                std::vector<net::steady_timer*>& list;
                net::steady_timer* timer;
                ~TimerGuard() { list.erase(std::remove(list.begin(), list.end(), timer), list.end()); }
            } tg{st->retry_timers, &t};
            t.expires_after(backoff.next());
            boost::system::error_code ec;
            co_await t.async_wait(net::redirect_error(net::use_awaitable, ec));
            if (ec || st->stopped) co_return;
        }
    }
    ++st->failures;
    st->write(spdlog::level::err, "alert delivery failed, giving up",
              {{"title", title}, {"attempts", std::to_string(attempts)}, {"error", last_error}});
}

}  // namespace

AlertSender::AlertSender(net::any_io_executor executor, AlertSenderOptions opts) : exec_(executor) {
    if (opts.url.empty()) throw std::invalid_argument("AlertSender: webhook url is empty");
    if (opts.token.empty()) throw std::invalid_argument("AlertSender: webhook token is empty");
    state_ = std::make_shared<State>(std::move(executor), std::move(opts));
}

void AlertSender::send(const Alert& alert) {
    if (state_->stopped) return;
    const auto seq = state_->seq++;
    net::co_spawn(exec_, deliver(state_, alert.title, alert_payload(state_->opts, alert), seq), net::detached);
}

void AlertSender::stop() {
    state_->stopped = true;
    for (auto* t : state_->retry_timers) t->cancel();
    state_->client.cancel();
}

std::uint64_t AlertSender::sent_total() const { return state_->sent.load(); }
std::uint64_t AlertSender::send_failures_total() const { return state_->failures.load(); }

}  // namespace evml::ops
