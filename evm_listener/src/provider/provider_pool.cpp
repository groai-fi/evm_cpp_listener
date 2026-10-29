#include "provider/provider_pool.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "core/error.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/url.hpp"

namespace net = boost::asio;

namespace evml::provider {

namespace {

using Clock = std::chrono::steady_clock;

const char* tag_name(Tag t) { return t == Tag::Safe ? "safe" : "finalized"; }
std::size_t tag_index(Tag t) { return static_cast<std::size_t>(t); }

const char* reason_name(ProviderBook::WsDecision::Reason r) {
    using R = ProviderBook::WsDecision::Reason;
    switch (r) {
        case R::Keep: return "keep";
        case R::Initial: return "initial";
        case R::Forced: return "failover";
        case R::SwitchBack: return "switch_back";
        case R::NoneAvailable: return "none_available";
    }
    return "unknown";
}

void validate(const PoolConfig& c) {
    const auto fail = [](const std::string& m) { throw std::invalid_argument("ProviderPool: " + m); };
    if (c.providers.empty()) fail("no providers");
    if (c.ws_order.empty()) fail("empty ws_order");
    if (c.getlogs_order.empty()) fail("empty getlogs_order");
    if (c.chain_id == 0) fail("chain_id not set");
    if (!c.subscribe_heads && !c.logs) fail("neither newHeads nor logs subscription configured");
    if (c.logs && (c.logs->addresses.empty() || c.logs->topic0s.empty())) fail("logs filter needs an address and a topic0");
    if (c.reconcile_rps == 0) fail("reconcile_rps must be >= 1");
    if (c.reconcile_burst < c.reconcile_rps) fail("reconcile_burst must be >= reconcile_rps");

    std::unordered_map<std::string, const ProviderEndpoint*> by_name;
    for (const auto& p : c.providers) {
        if (p.name.empty()) fail("provider with empty name");
        if (!by_name.emplace(p.name, &p).second) fail("duplicate provider name '" + p.name + "'");
    }
    const auto find = [&](const std::string& n, const char* list) -> const ProviderEndpoint& {
        const auto it = by_name.find(n);
        if (it == by_name.end()) fail(std::string(list) + " references unknown provider '" + n + "'");
        return *it->second;
    };
    for (const auto& n : c.ws_order) {
        const auto& p = find(n, "ws_order");
        if (p.ws_url.empty()) fail("provider '" + n + "' is in ws_order but has no ws_url");
        if (p.http_url.empty()) fail("provider '" + n + "' is in ws_order but has no http_url (needed for probes)");
        rpc::parse_url(p.ws_url, rpc::UrlKind::Ws);
    }
    for (const auto& n : c.getlogs_order) {
        const auto& p = find(n, "getlogs_order");
        if (p.http_url.empty()) fail("provider '" + n + "' is in getlogs_order but has no http_url");
    }
}

}  // namespace

std::shared_ptr<ProviderPool> ProviderPool::create(net::any_io_executor executor, PoolConfig cfg,
                                                   PoolHandlers handlers) {
    validate(cfg);
    return std::make_shared<ProviderPool>(Private{}, std::move(executor), std::move(cfg), std::move(handlers));
}

ProviderPool::ProviderPool(Private, net::any_io_executor executor, PoolConfig cfg, PoolHandlers handlers)
    : exec_(std::move(executor)),
      cfg_(std::move(cfg)),
      handlers_(std::move(handlers)),
      book_(cfg_.ws_order, cfg_.getlogs_order, cfg_.policy),
      detector_(Clock::now(), cfg_.stale),
      tick_timer_(exec_),
      restart_timer_(exec_) {
    std::uint64_t i = 0;
    for (const auto& p : cfg_.providers) {
        std::unique_ptr<rpc::HttpRpcClient> http;
        if (!p.http_url.empty()) {
            rpc::HttpRpcClientConfig hc;
            hc.name = p.name;
            hc.url = p.http_url;
            hc.user_agent = cfg_.user_agent;
            hc.timeout = cfg_.http_timeout;
            http = std::make_unique<rpc::HttpRpcClient>(exec_, std::move(hc));
        }
        nodes_.emplace(p.name, Node(p, std::move(http),
                                    rpc::Backoff(cfg_.seed + i++, cfg_.backoff_base, cfg_.backoff_cap,
                                                 cfg_.backoff_min_live),
                                    TokenBucket(cfg_.reconcile_rps, cfg_.reconcile_burst, Clock::now())));
        names_.push_back(p.name);
    }
}

void ProviderPool::start() {
    if (started_) throw std::logic_error("ProviderPool::start called twice");
    started_ = true;
    running_ = true;
    http_only_ = false;
    const auto now = Clock::now();
    next_poll_ = now;
    next_finalized_ = now;
    next_crosscheck_ = now + cfg_.crosscheck_period;
    for (auto& [_, n] : nodes_) n.next_standby_probe = now + cfg_.standby_probe_period;
    tick();
}

void ProviderPool::start_http_only() {
    if (started_) throw std::logic_error("ProviderPool::start_http_only called twice");
    started_ = true;
    running_ = true;
    http_only_ = true;
    const auto now = Clock::now();
    for (auto& [_, n] : nodes_) n.next_standby_probe = now + cfg_.standby_probe_period;
    schedule_tick();
}

void ProviderPool::stop() {
    running_ = false;
    stopped_ = true;
    tick_timer_.cancel();
    end_session_quiet();
    for (auto& [_, n] : nodes_) {
        for (auto* t : n.wait_timers) {
            t->cancel();
        }
        if (n.http) {
            n.http->cancel();
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// WebSocket supervisor
// ---------------------------------------------------------------------------------------------------------------

void ProviderPool::reconcile() {
    if (!running_ || http_only_) return;
    const auto now = Clock::now();
    // Query book for best usable WebSocket provider (handles sticky retention & switchback).
    const auto d = book_.select_ws(now);
    if (!d.changed) return;
    end_session_quiet();
    detector_.reset_ws(now);
    if (handlers_.on_ws_active_changed) handlers_.on_ws_active_changed(d.from, d.provider, reason_name(d.reason));
    if (d.provider) start_session(*d.provider);
}

void ProviderPool::end_session_quiet() {
    if (session_) {
        session_->stop();  // idempotent; suppresses every later callback
        session_.reset();
    }
    ++session_gen_;  // invalidates callbacks and restart timers that are already queued
    ws_ready_ = false;
    restart_timer_.cancel();
}

void ProviderPool::start_session(const std::string& name) {
    const auto gen = ++session_gen_;
    const Node& n = nodes_.at(name);

    rpc::WsSessionConfig c;
    c.name = name;
    c.url = n.ep.ws_url;
    c.user_agent = cfg_.user_agent;
    c.expected_chain_id = cfg_.chain_id;
    c.subscribe_heads = cfg_.subscribe_heads;
    c.logs = cfg_.logs;
    c.connect_timeout = cfg_.connect_timeout;
    c.handshake_timeout = cfg_.handshake_timeout;
    c.request_timeout = cfg_.request_timeout;
    c.subscribe_timeout = cfg_.subscribe_timeout;
    c.idle_timeout = cfg_.idle_timeout;

    // weak_ptr: the pool owns the session, the session must not keep the pool alive (no cycle).
    const auto w = weak_from_this();
    rpc::WsHandlers h;
    h.on_ready = [w, gen] {
        auto p = w.lock();
        if (!p || p->session_gen_ != gen) return;
        const auto now = Clock::now();
        p->ws_ready_ = true;
        p->ws_credited_ = false;
        p->ws_ready_at_ = now;
        p->detector_.reset_ws(now);  // the head-stall clock starts when the stream is actually live
        if (p->handlers_.on_ws_ready) p->handlers_.on_ws_ready(p->session_name_);
    };
    h.on_head = [w, gen](const rpc::Head& head) {
        auto p = w.lock();
        if (!p || p->session_gen_ != gen) return;
        p->detector_.on_ws_head(head.number, Clock::now());
        if (p->handlers_.on_head) p->handlers_.on_head(head, p->session_name_);
    };
    h.on_log = [w, gen](const rpc::Log& log) {
        auto p = w.lock();
        if (!p || p->session_gen_ != gen) return;
        if (p->handlers_.on_log) p->handlers_.on_log(log, p->session_name_);
    };
    h.on_closed = [w, gen](const rpc::WsClosed& closed) {
        auto p = w.lock();
        if (!p || p->session_gen_ != gen) return;
        p->end_session(closed);
    };

    session_ = rpc::WsSession::create(exec_, std::move(c), std::move(h));
    session_name_ = name;
    session_->start();
}

void ProviderPool::end_session(rpc::WsClosed c) {
    const auto now = Clock::now();
    const std::string name = session_name_;
    Node& n = nodes_.at(name);
    const auto live = ws_ready_ ? std::chrono::duration_cast<ms>(now - ws_ready_at_) : ms{0};

    end_session_quiet();
    n.backoff.report_live(live);
    if (handlers_.on_ws_down) handlers_.on_ws_down(c, name);
    remember_fault(name, c.kind, c.http_status, c.rpc_code, c.reason);
    // Record session failure in book; triggers transition to Down if failure threshold exceeded.
    if (const auto t = book_.on_failure(name, c.kind, c.retry_after, now)) emit_transition(*t, "ws: " + c.reason);

    reconcile();
    if (!running_ || session_) return;  // reconcile already started a session on another provider
    const auto& active = book_.active_ws();
    if (!active) return;                // nothing usable: tick() retries as providers recover
    // Apply backoff delay respecting Retry-After hint before reconnecting.
    const auto hint = c.retry_after ? std::chrono::duration_cast<ms>(*c.retry_after) : ms{0};
    schedule_restart(*active, std::max(nodes_.at(*active).backoff.next(), hint));
}

void ProviderPool::schedule_restart(const std::string& name, ms delay) {
    const auto gen = session_gen_;
    restart_timer_.expires_after(delay);
    restart_timer_.async_wait([w = weak_from_this(), gen, name](const boost::system::error_code& ec) {
        if (ec) return;
        auto p = w.lock();
        if (!p || !p->running_ || p->session_gen_ != gen || p->session_) return;
        if (p->book_.active_ws() != name) return;
        p->start_session(name);
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------------------------------------------

// Keeps why a provider went to Cooldown / Disabled / Fatal; other failure kinds are transient and not worth keeping.
void ProviderPool::remember_fault(const std::string& provider, rpc::FailKind kind, int http_status,
                                  std::optional<std::int64_t> rpc_code, const std::string& message) {
    if (kind != rpc::FailKind::QuotaExhausted && kind != rpc::FailKind::AuthFatal && kind != rpc::FailKind::ConfigFatal)
        return;
    nodes_.at(provider).fault = {http_status, rpc_code, message.substr(0, 200)};
}

std::optional<std::uint32_t> ProviderPool::ms_since_head() const {
    // Silence is measured across session restarts and failovers (even with no active WS provider): only a real head
    // resets it, so a pool stuck without heads still reports growing silence and WebSocket Stalled can fire.
    if (!running_ || !cfg_.subscribe_heads) return std::nullopt;
    const auto ms_silent = detector_.ws_head_silence(Clock::now()).count();
    return static_cast<std::uint32_t>(std::clamp<std::int64_t>(ms_silent, 0, std::numeric_limits<std::uint32_t>::max()));
}

std::vector<ProviderHealth> ProviderPool::health() const {
    const auto now = Clock::now();
    std::vector<ProviderHealth> out;
    out.reserve(names_.size());
    for (const auto& name : names_) {
        const auto& ps = book_.state(name);
        ProviderHealth h;
        h.name = name;
        h.state = ps.state;
        if (ps.state == State::Cooldown || ps.state == State::Disabled || ps.state == State::Fatal) {
            const auto& f = nodes_.at(name).fault;
            h.http_status = f.http_status;
            h.rpc_code = f.rpc_code;
            h.fault_message = f.message;
        }
        if (ps.state == State::Down || ps.state == State::Cooldown || ps.state == State::Disabled) {
            h.next_probe_in = std::max(ms{0}, std::chrono::duration_cast<ms>(ps.next_probe_at - now));
        }
        out.push_back(std::move(h));
    }
    return out;
}

void ProviderPool::emit_transition(const Transition& t, std::string_view reason) {
    if (running_ && handlers_.on_transition) handlers_.on_transition(t, reason);
}

void ProviderPool::schedule_tick() {
    tick_timer_.expires_after(cfg_.tick);
    tick_timer_.async_wait([w = weak_from_this()](const boost::system::error_code& ec) {
        if (ec) return;
        if (auto p = w.lock()) p->tick();
    });
}

void ProviderPool::tick() {
    if (!running_) return;
    const auto self = shared_from_this();
    const auto now = Clock::now();
    if (!http_only_) {
        reconcile();

        // A session that stayed Ready long enough counts as a success (a connect-then-die flap never does).
        // Mark provider healthy only if session survived backoff_min_live (anti-flap protection).
        if (session_ && ws_ready_ && !ws_credited_ && now - ws_ready_at_ >= cfg_.backoff_min_live) {
            ws_credited_ = true;
            if (const auto t = book_.on_success(session_name_, now)) emit_transition(*t, "ws session live");
        }

        // Detect silent stalls where WebSocket connection is open but no newHeads arrive.
        if (session_ && ws_ready_ && cfg_.subscribe_heads && detector_.ws_head_stalled(now)) {
            rpc::WsClosed c;
            c.kind = rpc::FailKind::Retry;
            c.reason = "no newHeads received (stalled stream)";
            end_session(std::move(c));
        } else if (const auto active = book_.active_ws(); active && detector_.ws_stale(now)) {
            // Demote only when another WS provider can take over: a lagging stream beats no stream.
            const bool alternative = std::any_of(cfg_.ws_order.begin(), cfg_.ws_order.end(), [&](const std::string& n) {
                return n != *active && book_.usable(n, now);
            });
            if (alternative) {
                if (const auto t = book_.demote_voluntary(*active, now)) {
                    emit_transition(*t, "ws head lagging the getLogs provider");
                    reconcile();
                }
            }
        }
    }

    // The getLogs provider may change whenever the WS provider or any health state changes.
    const auto active = book_.active_ws();
    const auto cur = book_.pick_getlogs(now);
    if (!http_only_) {
        if (cur != last_getlogs_) {
            detector_.reset_http(now);
            last_getlogs_ = cur;
        }
        // Demote getLogs provider voluntarily if its safe block tag is lagging active WS head.
        if (cur && detector_.getlogs_stale(now) && book_.pick_getlogs(now, {*cur})) {
            if (const auto t = book_.demote_voluntary(*cur, now)) {
                emit_transition(*t, "getLogs provider's safe tag lagging the WS provider");
                detector_.reset_http(now);
            }
        }
    }

    for (const auto& name : names_) {
        if (probing_.count(name) != 0) continue;
        const State st = book_.state(name).state;
        if (st == State::Fatal) continue;
        Node& n = nodes_.at(name);
        bool due;
        if (st == State::Up) {
            if (http_only_ || active == name || cur == name) continue;  // carries real traffic: that is its health signal
            due = now >= n.next_standby_probe;
        } else {
            due = book_.probe_due(name, now);
        }
        if (!due) continue;
        n.next_standby_probe = now + cfg_.standby_probe_period;
        probing_.insert(name);
        net::co_spawn(exec_, probe(self, name), net::detached);
    }

    if (!http_only_) {
        if (cur && !polling_ && now >= next_poll_) {
            polling_ = true;
            next_poll_ = now + cfg_.safe_poll;
            net::co_spawn(exec_, poll(self, *cur), net::detached);
        }
        if (active && cur && *active != *cur && !crosschecking_ && now >= next_crosscheck_) {
            crosschecking_ = true;
            next_crosscheck_ = now + cfg_.crosscheck_period;
            net::co_spawn(exec_, crosscheck(self, *active), net::detached);
        }
    }

    schedule_tick();
}

// Folds the outcome of real traffic into the provider's health. RangeTooLarge is not a provider failure.
void ProviderPool::note(const std::string& provider, const RpcOutcome& o) {
    const auto now = Clock::now();
    std::optional<Transition> t;
    if (o.ok) {
        // A response that was in flight when the provider failed must not undo Down/Cooldown/Disabled: those states
        // are left only through a probe (see provider_book.hpp).
        if (book_.state(provider).state == State::Up) t = book_.on_success(provider, now);
    } else if (o.kind != rpc::FailKind::RangeTooLarge) {
        remember_fault(provider, o.kind, o.http_status, o.rpc_code, o.error);
        t = book_.on_failure(provider, o.kind, o.retry_after, now);
    }
    if (t) {
        emit_transition(*t, o.ok ? "http request ok" : "http: " + o.error);
        reconcile();
    }
}

// ---------------------------------------------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------------------------------------------

// One attempt on one provider. Never throws for provider/network trouble: that is reported in the outcome.
net::awaitable<RpcOutcome> ProviderPool::call_on(std::string provider, std::string body, std::uint64_t id) {
    const auto self = shared_from_this();
    RpcOutcome o;
    o.provider = provider;
    Node& n = nodes_.at(provider);
    if (!n.http) throw std::logic_error("ProviderPool: provider '" + provider + "' has no http_url");
    for (const char* m : {"eth_getLogs", "eth_getBlockByNumber", "eth_blockNumber", "eth_chainId"}) {
        if (body.find(m) != std::string::npos) {
            ++n.rpc_requests[m];
            break;
        }
    }

    rpc::HttpResult r;
    try {
        r = co_await n.http->call(std::move(body));
    } catch (const rpc::TransportError& e) {
        o.kind = rpc::FailKind::Retry;
        o.error = e.what();
        co_return o;
    }

    const auto apply = [&](const rpc::ErrorInfo& info, const std::string& what) {
        const auto c = rpc::classify(info);
        o.kind = c.kind;
        o.range_limit = c.range_limit;
        o.retry_after = c.retry_after;
        o.http_status = info.http_status;
        o.rpc_code = info.rpc_code;
        o.error = what + ": " + info.message;
    };

    if (r.status < 200 || r.status >= 300) {
        apply(rpc::error_info_from_http(r, dec_), "http " + std::to_string(r.status));
        co_return o;
    }

    rpc::Message m;
    try {
        m = dec_.decode(r.body);
    } catch (const core::DecodeError& e) {
        o.kind = rpc::FailKind::Retry;
        o.error = std::string("undecodable response: ") + e.what();
        co_return o;
    }
    const auto* resp = std::get_if<rpc::Response>(&m);
    if (resp == nullptr) {
        o.kind = rpc::FailKind::Retry;
        o.error = "notification received instead of a response";
        co_return o;
    }
    if (!resp->id || *resp->id != id) {
        o.kind = rpc::FailKind::Retry;
        o.error = "response id mismatch";
        co_return o;
    }
    if (!resp->ok) {
        rpc::ErrorInfo info;
        info.http_status = r.status;
        info.rpc_code = resp->error.code;
        info.message = resp->error.message;
        info.retry_after = r.retry_after;
        apply(info, "rpc error " + std::to_string(resp->error.code));
        co_return o;
    }
    o.ok = true;
    o.result_raw = resp->result_raw;
    co_return o;
}

net::awaitable<RpcOutcome> ProviderPool::call(RequestBuilder build) {
    const auto self = shared_from_this();
    std::unordered_set<std::string> tried;
    RpcOutcome last;
    if (stopped_) {
        last.error = "pool stopped";
        co_return last;
    }
    last.error = "no getLogs provider is usable";
    for (;;) {
        if (stopped_) {
            last.ok = false;
            last.error = "pool stopped";
            co_return last;
        }
        // Pick next usable HTTP provider, skipping already tried endpoints during this request.
        const auto p = book_.pick_getlogs(Clock::now(), tried);
        if (!p) co_return last;
        tried.insert(*p);
        const auto id = next_id_++;
        auto o = co_await call_on(*p, build(id), id);
        if (stopped_) {
            o.ok = false;
            o.error = "pool stopped";
            co_return o;
        }
        note(*p, o);
        // Return immediately on success or RangeTooLarge (caller adjusts range without provider failover).
        if (o.ok || o.kind == rpc::FailKind::RangeTooLarge) co_return o;
        last = std::move(o);
    }
}

net::awaitable<RpcOutcome> ProviderPool::call_to(std::string provider, RequestBuilder build) {
    const auto self = shared_from_this();
    Node& n = nodes_.at(provider);  // nodes_ is never modified after construction: the reference stays valid
    if (stopped_) {
        RpcOutcome o;
        o.provider = provider;
        o.error = "pool stopped";
        co_return o;
    }
    const auto wait = n.bucket.reserve(Clock::now());
    if (wait > Clock::duration::zero()) {
        ++n.throttled;
        net::steady_timer timer(exec_, wait);
        n.wait_timers.push_back(&timer);
        struct TimerGuard {
            std::vector<net::steady_timer*>& list;
            net::steady_timer* t;
            ~TimerGuard() { list.erase(std::remove(list.begin(), list.end(), t), list.end()); }
        } guard{n.wait_timers, &timer};
        boost::system::error_code ec;
        co_await timer.async_wait(net::redirect_error(net::use_awaitable, ec));
        if (stopped_ || ec) {
            RpcOutcome o;
            o.provider = provider;
            o.error = "pool stopped";
            co_return o;
        }
    }
    const auto id = next_id_++;
    auto o = co_await call_on(provider, build(id), id);
    if (stopped_) {
        o.ok = false;
        o.error = "pool stopped";
        co_return o;
    }
    note(provider, o);
    co_return o;
}

void ProviderPool::report_bad(const std::string& provider, const std::string& reason) {
    ++nodes_.at(provider).inconsistent;
    if (const auto t = book_.on_failure(provider, rpc::FailKind::Retry, std::nullopt, Clock::now())) {
        emit_transition(*t, "bad response: " + reason);
        reconcile();
    }
}

net::awaitable<std::optional<TagSnapshot>> ProviderPool::read_tag(std::string provider, Tag tag) {
    const auto self = shared_from_this();
    if (stopped_) co_return std::nullopt;
    const auto id = next_id_++;
    // Fetch block by tag name ("safe" or "finalized") via eth_getBlockByNumber.
    auto o = co_await call_on(provider, rpc::build_get_block_by_number(id, tag_name(tag), false), id);
    if (stopped_) co_return std::nullopt;

    std::optional<rpc::Head> head;
    if (o.ok) {
        try {
            head = dec_.parse_block(o.result_raw);
            if (!head) {
                // Node returned null result for tag; counts as a retryable provider error.
                o.ok = false;
                o.kind = rpc::FailKind::Retry;
                o.error = std::string("null block for tag '") + tag_name(tag) + "'";
            }
        } catch (const core::DecodeError& e) {
            o.ok = false;
            o.kind = rpc::FailKind::Retry;
            o.error = std::string("undecodable block: ") + e.what();
        }
    }
    note(provider, o);
    if (!o.ok) co_return std::nullopt;

    TagSnapshot s;
    s.provider = provider;
    s.number = head->number;
    s.hash = head->hash;
    s.at = Clock::now();
    // Cache tag snapshot in node record to avoid redundant network calls.
    nodes_.at(provider).tags[tag_index(tag)] = s;
    if (running_ && handlers_.on_tag) handlers_.on_tag(provider, tag, s.number, s.hash);
    co_return s;
}

std::optional<TagSnapshot> ProviderPool::cached_tag(const std::string& provider, Tag tag) const {
    return nodes_.at(provider).tags[tag_index(tag)];
}

net::awaitable<void> ProviderPool::probe([[maybe_unused]] std::shared_ptr<ProviderPool> self, std::string provider) {
    const auto id = next_id_++;
    // Probe node liveness via lightweight eth_blockNumber call.
    auto o = co_await call_on(provider, rpc::build_block_number(id), id);
    if (o.ok) {
        try {
            dec_.parse_quantity(o.result_raw);
        } catch (const core::DecodeError& e) {
            o.ok = false;
            o.kind = rpc::FailKind::Retry;
            o.error = std::string("undecodable block number: ") + e.what();
        }
    }
    probing_.erase(provider);
    if (!running_) co_return;

    const auto now = Clock::now();
    std::optional<Transition> t;
    if (o.ok) {
        // Successful probe restores Down provider to Up.
        t = book_.on_probe_result(provider, true, now);
    } else {
        switch (o.kind) {
            case rpc::FailKind::QuotaExhausted:
            case rpc::FailKind::AuthFatal:
            case rpc::FailKind::ConfigFatal:
                remember_fault(provider, o.kind, o.http_status, o.rpc_code, o.error);
                t = book_.on_failure(provider, o.kind, o.retry_after, now);
                break;
            default:
                t = book_.on_probe_result(provider, false, now);
                break;
        }
    }
    if (t) {
        emit_transition(*t, o.ok ? "probe ok" : "probe failed: " + o.error);
        reconcile();
    }
}

net::awaitable<void> ProviderPool::poll([[maybe_unused]] std::shared_ptr<ProviderPool> self, std::string provider) {
    struct Done {
        ProviderPool& p;
        ~Done() { p.polling_ = false; }
    } done{*this};

    const auto id = next_id_++;
    auto o = co_await call_on(provider, rpc::build_block_number(id), id);
    std::optional<std::uint64_t> head;
    if (o.ok) {
        try {
            head = dec_.parse_quantity(o.result_raw);
        } catch (const core::DecodeError& e) {
            o.ok = false;
            o.kind = rpc::FailKind::Retry;
            o.error = std::string("undecodable block number: ") + e.what();
        }
    }
    if (!running_) co_return;
    note(provider, o);
    if (!head) co_return;
    // Feed latest HTTP head block to StaleDetector for cross-provider divergence checking.
    if (last_getlogs_ == provider) detector_.on_http_head(*head, Clock::now());

    const auto safe = co_await read_tag(provider, Tag::Safe);
    if (running_ && safe && last_getlogs_ == provider) detector_.on_http_safe(safe->number, Clock::now());

    if (running_ && Clock::now() >= next_finalized_) {
        next_finalized_ = Clock::now() + cfg_.finalized_poll;
        co_await read_tag(provider, Tag::Finalized);
    }
}

net::awaitable<void> ProviderPool::crosscheck([[maybe_unused]] std::shared_ptr<ProviderPool> self,
                                              std::string provider) {
    struct Done {
        ProviderPool& p;
        ~Done() { p.crosschecking_ = false; }
    } done{*this};

    const auto safe = co_await read_tag(provider, Tag::Safe);
    // Only meaningful while that provider is still the WS provider (the detector was reset when it changed).
    if (running_ && safe && book_.active_ws() == provider) detector_.on_ws_safe(safe->number, Clock::now());
}

}  // namespace evml::provider
