#include "app/tools.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "app/host.hpp"
#include "app/pool_config.hpp"
#include "app/pool_log_source.hpp"
#include "core/env.hpp"
#include "core/error.hpp"
#include "core/keccak.hpp"
#include "core/log.hpp"
#include "core/version.hpp"
#include "pipeline/aimd_pager.hpp"
#include "pipeline/coverage.hpp"
#include "pipeline/event.hpp"
#include "pipeline/log_validation.hpp"
#include "pipeline/sweeper.hpp"
#include "provider/provider_pool.hpp"
#include "rpc/backoff.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/codec.hpp"
#include "sink/keys.hpp"
#include "sink/lease.hpp"
#include "sink/redispp_conn.hpp"
#include "sink/redis_sink.hpp"
#include "sink/redis_startup.hpp"

namespace evml::app {

/// @brief Replaces `{version}` placeholder in template with compiled semver string.
std::string user_agent(const core::Config& cfg) {
    std::string ua = cfg.ops.http_user_agent;
    const std::string token = "{version}";
    for (auto pos = ua.find(token); pos != std::string::npos; pos = ua.find(token, pos)) {
        ua.replace(pos, token.size(), core::kSemver);
        pos += std::strlen(core::kSemver);
    }
    return ua;
}

namespace {

using core::Address20;
using core::Config;
using core::Provider;

/// @brief Exception thrown when an RPC provider responds with an HTTP error or JSON-RPC error payload.
struct RpcFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// @brief Truncates error messages to avoid console flooding on large response bodies.
std::string clip(std::string_view s, std::size_t n = 120) {
    return std::string(s.substr(0, n));
}

/// @brief Executes a single blocking JSON-RPC call over HTTP against a specific provider.
/// @return Raw JSON result payload.
/// @throws TransportError On network I/O or connect failure.
/// @throws RpcFailure On non-200 HTTP code or JSON-RPC error object.
/// @throws DecodeError On invalid JSON structure.
std::string rpc_call(const Config& cfg, const Provider& p, const std::string& body, rpc::Decoder& dec) {
    // Dispatch blocking HTTP POST with configured backfill timeout and evaluated user agent.
    const auto resp = rpc::http_post_json(p.http_url, body, std::chrono::milliseconds(cfg.backfill.request_timeout_ms),
                                          user_agent(cfg));
    // Check HTTP status code; parse JSON-RPC error payload if provided in response body.
    if (resp.status != 200) {
        if (const auto err = dec.try_parse_error_body(resp.body)) {
            throw RpcFailure(p.name + ": HTTP " + std::to_string(resp.status) + " rpc error " +
                             std::to_string(err->code) + ": " + clip(err->message));
        }
        throw RpcFailure(p.name + ": HTTP " + std::to_string(resp.status) + ": " + clip(resp.body));
    }
    // Decode JSON-RPC message envelope.
    const auto msg = dec.decode(resp.body);
    const auto* r = std::get_if<rpc::Response>(&msg);
    if (!r) throw RpcFailure(p.name + ": unexpected notification in HTTP response");
    if (!r->ok) throw RpcFailure(p.name + ": rpc error " + std::to_string(r->error.code) + ": " + clip(r->error.message));
    return r->result_raw;
}

/// @brief Determines whether bytecode exists based on the hex string length ("0x" means empty).
bool has_code(const std::string& code_hex) {
    // "0x" = no code; anything longer must be 0x + hex.
    return code_hex.size() > 2;
}

/// @brief Queries eth_getCode for an address at a specified block with automatic provider failover.
bool code_at(const Config& cfg, const Address20& addr, const std::string& block, rpc::Decoder& dec,
             std::uint64_t id) {
    std::string failures;
    // Iterate through configured getLogs provider priority chain until one succeeds.
    for (const auto& p : cfg.getlogs_chain) {
        try {
            const auto raw = rpc_call(cfg, p, rpc::build_get_code(id, addr, block), dec);
            return has_code(dec.parse_string(raw));
        } catch (const std::exception& e) {
            failures += "\n  - " + std::string(e.what());
        }
    }
    throw std::runtime_error("eth_getCode " + addr.to_hex() + " @" + block + " failed on every provider:" + failures);
}

}  // namespace

/// @brief Prints human-readable configuration summary with all secrets and endpoint tokens redacted.
void print_config_summary(const Config& cfg) {
    std::cout << "shard:    " << cfg.shard.id << " (host " << cfg.shard.host_id << ")\n";
    std::cout << "chain:    " << cfg.chain.name << " id=" << cfg.chain.chain_id << " block_time_ms="
              << cfg.chain.block_time_ms << " start_policy="
              << (cfg.chain.start_policy == core::StartPolicy::Gate ? "gate" : "contract_start") << "\n";
    std::cout << "ws providers:\n";
    // Redact tokens/credentials from printed provider URLs.
    for (const auto& p : cfg.ws_chain) std::cout << "  " << p.name << "  " << core::env::redact(p.ws_url) << "\n";
    std::cout << "getlogs providers:\n";
    for (const auto& p : cfg.getlogs_chain) {
        std::cout << "  " << p.name << "  " << core::env::redact(p.http_url) << "  max_range=" << p.max_getlogs_range
                  << "\n";
    }
    for (const auto& n : cfg.notes) std::cout << "note: " << n << "\n";
    std::cout << "contracts:\n";
    for (const auto& c : cfg.contracts) {
        std::cout << "  " << c.name << " " << c.address.to_hex() << " finality=" << core::to_string(c.finality)
                  << " stream=" << core::to_string(c.stream) << " start_block="
                  << (c.start_block ? std::to_string(*c.start_block) : std::string("gate")) << "\n";
        for (std::size_t i = 0; i < c.events.size(); ++i) {
            std::cout << "    " << c.events[i] << "  " << c.topics[i].to_hex() << "\n";
        }
    }
    std::cout << "routes: " << cfg.routes.size() << "\n";
}

/// @brief Performs pre-flight live validation against configured RPC providers.
int check_config_online(const Config& cfg) {
    rpc::Decoder dec;
    std::uint64_t id = 1;
    int working = 0;
    bool fatal = false;

    // Verify eth_chainId across all declared getLogs providers.
    for (const auto& p : cfg.getlogs_chain) {
        try {
            const auto raw = rpc_call(cfg, p, rpc::build_chain_id(id++), dec);
            const auto got = dec.parse_quantity(raw);
            // Invariant: Provider chain ID must strictly match configured chain ID to prevent cross-chain corruption.
            if (got != cfg.chain.chain_id) {
                std::cerr << "FATAL " << p.name << ": eth_chainId=" << got << " but config chain_id="
                          << cfg.chain.chain_id << "\n";
                fatal = true;
            } else {
                std::cout << "ok    " << p.name << ": eth_chainId=" << got << "\n";
                ++working;
            }
        } catch (const std::exception& e) {
            std::cerr << "WARN  " << e.what() << "\n";
        }
    }
    if (fatal) return kExitFailure;
    if (working == 0) {
        std::cerr << "FATAL no getLogs provider answered eth_chainId\n";
        return kExitFailure;
    }

    int missing = 0;
    // Verify that every tracked contract exists (has non-empty bytecode) on the target network.
    for (const auto& c : cfg.contracts) {
        try {
            if (code_at(cfg, c.address, "latest", dec, id++)) {
                std::cout << "ok    " << c.name << " has code at latest\n";
            } else {
                std::cerr << "FAIL  " << c.name << " " << c.address.to_hex() << " has NO code at latest\n";
                ++missing;
            }
        } catch (const std::exception& e) {
            std::cerr << "FAIL  " << c.name << ": " << e.what() << "\n";
            ++missing;
        }
    }
    return missing == 0 ? kExitOk : kExitFailure;
}

/// @brief Discovers the exact deployment block of a contract via binary search over eth_getCode.
std::uint64_t find_deploy_block(const Config& cfg, const Address20& address) {
    rpc::Decoder dec;
    std::uint64_t id = 1;

    std::uint64_t head = 0;
    {
        std::string failures;
        bool got = false;
        // Obtain current chain tip block number to establish upper search bound.
        for (const auto& p : cfg.getlogs_chain) {
            try {
                head = dec.parse_quantity(rpc_call(cfg, p, rpc::build_block_number(id++), dec));
                got = true;
                break;
            } catch (const std::exception& e) {
                failures += "\n  - " + std::string(e.what());
            }
        }
        if (!got) throw std::runtime_error("eth_blockNumber failed on every provider:" + failures);
    }

    // Verify contract has code at head before commencing binary search.
    if (!code_at(cfg, address, core::to_hex_u64(head), dec, id++)) {
        throw std::runtime_error(address.to_hex() + " has no code at head block " + std::to_string(head));
    }
    // Invariant: no code at `lo` (block 0 is assumed codeless for any non-genesis contract), code at `hi`.
    std::uint64_t lo = 0, hi = head;
    // Edge case: contract existed at genesis block 0.
    if (code_at(cfg, address, core::to_hex_u64(lo), dec, id++)) return 0;
    // Binary search over block range [lo, hi] in O(log N) queries.
    while (hi - lo > 1) {
        const std::uint64_t mid = lo + (hi - lo) / 2;
        if (code_at(cfg, address, core::to_hex_u64(mid), dec, id++)) {
            hi = mid;  // Bytecode exists; deployment block is at or before mid.
        } else {
            lo = mid;  // No bytecode; deployment block is strictly after mid.
        }
    }
    return hi;
}

/// @brief Runs a live, end-to-end transport probe using real WebSocket and HTTP connections.
int probe_transport(const Config& cfg, unsigned seconds) {
    namespace net = boost::asio;
    core::log::init("info");

    struct Stats {
        std::size_t heads{0}, logs{0};
        std::uint64_t last_head{0};
        std::string head_from;
        bool ok{false};
    } st;

    provider::PoolHandlers h;
    // Track streamed block heads and logging provider identity.
    h.on_head = [&st](const rpc::Head& x, const std::string& p) {
        ++st.heads;
        st.last_head = x.number;
        st.head_from = p;
    };
    h.on_log = [&st](const rpc::Log&, const std::string&) { ++st.logs; };
    h.on_ws_active_changed = [](const std::optional<std::string>&, const std::optional<std::string>& p,
                                std::string_view why) {
        std::cout << "ws active: " << (p ? *p : std::string("none")) << " (" << why << ")\n";
    };
    h.on_ws_down = [](const rpc::WsClosed& c, const std::string& p) {
        std::cout << "ws down:   " << p << " [" << rpc::to_string(c.kind) << "] " << c.reason << "\n";
    };
    h.on_transition = [](const provider::Transition& t, std::string_view why) {
        std::cout << "provider:  " << t.provider << " " << provider::to_string(t.from) << " -> "
                  << provider::to_string(t.to) << " (" << why << ")\n";
    };

    net::io_context ioc;
    auto pool = provider::ProviderPool::create(ioc.get_executor(), make_pool_config(cfg, user_agent(cfg)), std::move(h));
    pool->start();

    // Spawn coroutine to stream heads for `seconds` and issue post-stream verification queries.
    net::co_spawn(
        ioc,
        [&]() -> net::awaitable<void> {
            net::steady_timer t(ioc);
            t.expires_after(std::chrono::seconds(seconds));
            co_await t.async_wait(net::use_awaitable);

            std::cout << "streamed: " << st.heads << " heads (last " << st.last_head << " via " << st.head_from
                      << "), " << st.logs << " logs\n";
            bool good = st.heads > 0;
            // Verify historical getLogs query across the 2 blocks preceding the latest streamed head.
            if (st.last_head >= 2) {
                const auto from = st.last_head - 2, to = st.last_head;
                const auto filter = cfg.filter();
                const auto o = co_await pool->call(
                    [&](std::uint64_t id) { return rpc::build_get_logs(id, from, to, filter); });
                if (o.ok) {
                    rpc::Decoder dec;
                    std::cout << "getLogs:   ok via " << o.provider << ", blocks " << from << ".." << to << ": "
                              << dec.parse_logs(o.result_raw).size() << " logs\n";
                } else {
                    std::cout << "getLogs:   FAILED (" << rpc::to_string(o.kind) << ") via " << o.provider << ": "
                              << o.error << "\n";
                    good = false;
                }
                // Verify that both safe and finalized block tags can be resolved.
                for (const auto tag : {provider::Tag::Safe, provider::Tag::Finalized}) {
                    const auto who = pool->current_getlogs();
                    std::optional<provider::TagSnapshot> s;
                    if (who) s = co_await pool->read_tag(*who, tag);
                    std::cout << (tag == provider::Tag::Safe ? "safe:      " : "finalized: ");
                    if (s) {
                        std::cout << s->number << " via " << s->provider << "\n";
                    } else {
                        std::cout << "FAILED\n";
                        good = false;
                    }
                }
            } else {
                std::cout << "getLogs:   skipped (no head received)\n";
            }
            st.ok = good;
            // Tear down pool resources and shut down Boost.Asio event loop.
            pool->stop();
            ioc.stop();
        },
        net::detached);
    ioc.run();
    return st.ok ? kExitOk : kExitFailure;
}

int check_redis(const Config& cfg) {
    const std::string redis_url = core::env::get(cfg.sink.redis_url_env);
    if (redis_url.empty()) {
        std::cerr << "FAIL  environment variable " << cfg.sink.redis_url_env << " is not set\n";
        return kExitFailure;
    }

    bool failed = false;
    std::string wait_error;
    std::string wait_family;
    sink::StartupOptions opts;
    opts.require_noeviction = cfg.sink.require_noeviction;
    opts.on_waiting = [&](const std::string& err, const std::string& fam, std::chrono::milliseconds) {
        wait_error = err;
        wait_family = fam;
        failed = true;
    };
    opts.stop = [&] { return failed; };
    opts.sleep = [](std::chrono::milliseconds) {};

    try {
        const auto rep = sink::await_redis_ready(redis_url, opts);
        if (!rep) {
            std::cerr << "FAIL  redis unreachable (address_family=" << wait_family << "): " << wait_error << "\n";
            return kExitFailure;
        }
        std::cout << "ok    redis version=" << rep->version.major << "." << rep->version.minor << "." << rep->version.patch
                  << " address_family=" << rep->address_family
                  << " maxmemory_policy=" << (rep->maxmemory_policy.empty() ? "unknown" : rep->maxmemory_policy)
                  << " appendonly=" << (rep->appendonly.empty() ? "unknown" : rep->appendonly) << "\n";
        for (const auto& w : rep->warnings) std::cout << "warn  " << w << "\n";
        return kExitOk;
    } catch (const sink::StartupFatal& e) {
        std::cerr << "FATAL " << e.what() << "\n";
        return kExitFailure;
    } catch (const std::exception& e) {
        std::cerr << "FAIL  " << e.what() << "\n";
        return kExitFailure;
    }
}

namespace {

Config single_contract_config(const Config& base, const core::ContractCfg& target, core::Finality finality) {
    Config out = base;
    core::ContractCfg one = target;
    one.finality = finality;
    out.contracts = {one};
    out.routes.clear();
    for (const auto& [k, v] : base.routes) {
        if (k.address == target.address) {
            core::EventRoute r = v;
            r.finality = finality;
            out.routes.emplace(k, std::move(r));
        }
    }
    return out;
}

std::uint64_t wall_now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

int backfill_contract(const Config& cfg, std::string_view contract_name, std::chrono::milliseconds lease_timeout) {
    namespace net = boost::asio;
    core::log::init("info");

    const core::ContractCfg* target = nullptr;
    for (const auto& c : cfg.contracts) {
        if (c.name == contract_name) {
            target = &c;
            break;
        }
    }
    if (!target) {
        throw core::ConfigError("unknown contract '" + std::string(contract_name) + "'");
    }

    const std::string redis_url = core::env::get(cfg.sink.redis_url_env);
    if (redis_url.empty()) {
        std::cerr << "FAIL  environment variable " << cfg.sink.redis_url_env << " is not set\n";
        return kExitFailure;
    }

    const auto pool_cfg = make_pool_config(cfg, user_agent(cfg));

    const auto deadline = std::chrono::steady_clock::now() + lease_timeout;
    const auto sleep_until_deadline = [&](std::chrono::milliseconds d) {
        const auto now = std::chrono::steady_clock::now();
        if (now < deadline) {
            std::this_thread::sleep_for(std::min(d, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
        }
    };

    sink::StartupOptions so;
    so.require_noeviction = cfg.sink.require_noeviction;
    so.stop = [&] { return std::chrono::steady_clock::now() >= deadline; };
    so.sleep = sleep_until_deadline;
    try {
        const auto rep = sink::await_redis_ready(redis_url, so);
        if (!rep) {
            std::cerr << "FAIL  redis unreachable within timeout\n";
            return kExitFailure;
        }
    } catch (const sink::StartupFatal& e) {
        std::cerr << "FATAL " << e.what() << "\n";
        return kExitFailure;
    }

    const sink::Keys keys{cfg.shard.id};
    std::string token;
    if (cfg.lease.enabled) {
        sink::RedisppConn conn(redis_url, keys);
        sink::AcquireOptions ao;
        ao.host_id = cfg.shard.host_id;
        ao.ttl_ms = cfg.lease.ttl_ms;
        ao.poll = std::min<std::chrono::milliseconds>(std::chrono::milliseconds(cfg.lease.renew_ms),
                                                      std::chrono::milliseconds(250));
        ao.stop = [&] { return std::chrono::steady_clock::now() >= deadline; };
        ao.sleep = sleep_until_deadline;
        const auto grant = sink::acquire_lease_blocking(conn, keys, ao);
        if (!grant) {
            std::cerr << "FAIL  could not acquire lease for shard " << cfg.shard.id
                      << " within timeout (held by another host)\n";
            return kExitFailure;
        }
        token = grant->token;
    } else {
        token = sink::lease_disabled_token();
    }

    auto sopts = sink::RedisSinkOptions::from_config(cfg.sink, cfg.lease, token);
    sink::RedisSink sink(std::move(sopts), keys, std::make_unique<sink::RedisppConn>(redis_url, keys),
                         std::make_unique<sink::RedisppConn>(redis_url, keys));
    sink.start();

    net::io_context ioc;
    auto pool = provider::ProviderPool::create(ioc.get_executor(), pool_cfg, {});
    pool->start_http_only();
    PoolLogSource source(pool, nullptr);

    pipeline::SweeperOptions sw_opt;
    sw_opt.min_range = cfg.backfill.min_range;
    sw_opt.max_page_logs = std::max<std::size_t>(1, cfg.sink.queue_capacity / 2);
    const pipeline::EventContext ectx{cfg.shard.id, cfg.shard.host_id, core::version_string()};

    int rc = kExitOk;
    std::optional<std::vector<std::string>> ck_contracts;

    net::co_spawn(
        ioc,
        [&]() -> net::awaitable<void> {
            const auto ck = co_await sink.load_checkpoint();
            if (!ck || !ck->safe || !ck->finalized || !ck->contracts || *ck->finalized > *ck->safe) {
                std::cerr << "FAIL  checkpoint is missing or incomplete in Redis\n";
                rc = kExitFailure;
                pool->stop();
                ioc.stop();
                co_return;
            }
            ck_contracts = ck->contracts;
            const std::uint64_t c_f = *ck->finalized;
            const std::uint64_t c_s = *ck->safe;

            const auto drive = [&](auto step_fn) -> net::awaitable<bool> {
                rpc::Backoff backoff(1, std::chrono::milliseconds(100), std::chrono::milliseconds(5000));
                std::uint32_t retries = 0;
                for (;;) {
                    if (sink.mode() == sink::SinkMode::Fenced || sink.mode() == sink::SinkMode::Failed) {
                        std::cerr << "FAIL  sink entered " << sink::to_string(sink.mode()) << " during backfill\n";
                        co_return false;
                    }
                    const pipeline::StepResult r = co_await step_fn();
                    if (r.status == pipeline::StepStatus::NoWork) co_return true;
                    if (r.status == pipeline::StepStatus::Advanced) {
                        backoff.reset();
                        retries = 0;
                        continue;
                    }
                    if (r.status == pipeline::StepStatus::Stalled) {
                        std::cerr << "FAIL  backfill stalled: " << r.detail << "\n";
                        co_return false;
                    }
                    if (r.status == pipeline::StepStatus::Retry && ++retries > 10) {
                        std::cerr << "FAIL  backfill exceeded retry limit: " << r.detail << "\n";
                        co_return false;
                    }
                    net::steady_timer timer(ioc);
                    timer.expires_after(r.status == pipeline::StepStatus::Paused ? std::chrono::milliseconds(100)
                                                                                 : backoff.next());
                    co_await timer.async_wait(net::use_awaitable);
                }
            };

            if (target->start_block && *target->start_block <= c_s) {
                const std::uint64_t start = *target->start_block;
                const std::uint64_t start_cursor = start > 0 ? start - 1 : 0;
                if (start <= c_f) {
                    const auto final_cfg = single_contract_config(cfg, *target, core::Finality::Finalized);
                    pipeline::Sweeper sw(final_cfg, ectx, source, sink, wall_now_ms, sw_opt);
                    sw.set_cursors(start_cursor, start_cursor);
                    if (!(co_await drive([&] { return sw.backfill_step(c_f, core::Finality::Finalized); }))) {
                        rc = kExitFailure;
                        pool->stop();
                        ioc.stop();
                        co_return;
                    }
                }
                if (target->finality == core::Finality::Safe && c_s > c_f) {
                    const std::uint64_t safe_cursor = std::max<std::uint64_t>(start_cursor, c_f);
                    if (safe_cursor < c_s) {
                        const auto safe_cfg = single_contract_config(cfg, *target, core::Finality::Safe);
                        pipeline::Sweeper sw(safe_cfg, ectx, source, sink, wall_now_ms, sw_opt);
                        sw.set_cursors(safe_cursor, std::min(safe_cursor, c_f));
                        if (!(co_await drive([&] { return sw.backfill_step(c_s, core::Finality::Safe); }))) {
                            rc = kExitFailure;
                            pool->stop();
                            ioc.stop();
                            co_return;
                        }
                    }
                }
            }
            pool->stop();
            ioc.stop();
        },
        net::detached);
    ioc.run();

    if (rc != kExitOk || !ck_contracts) {
        sink.drain(std::chrono::seconds(5));
        return kExitFailure;
    }

    // Update the manifest only after the last page is acknowledged (§5.6a).
    if (!sink.wait_idle(std::chrono::seconds(30)) || sink.mode() != sink::SinkMode::Connected) {
        std::cerr << "FAIL  sink did not acknowledge backfill emissions\n";
        sink.drain(std::chrono::seconds(5));
        return kExitFailure;
    }

    std::vector<std::string> updated = *ck_contracts;
    const std::string prefix = target->address.to_hex() + ":";
    updated.erase(
        std::remove_if(updated.begin(), updated.end(), [&](const std::string& e) { return e.rfind(prefix, 0) == 0; }),
        updated.end());
    const auto one_manifest = pipeline::contract_manifest(single_contract_config(cfg, *target, target->finality));
    updated.insert(updated.end(), one_manifest.begin(), one_manifest.end());
    std::sort(updated.begin(), updated.end());
    sink.set_contracts(std::move(updated));

    const auto drained = sink.drain(std::chrono::seconds(25));
    if (!drained.flushed || sink.mode() != sink::SinkMode::Connected) {
        std::cerr << "FAIL  sink failed to flush updated contract manifest\n";
        return kExitFailure;
    }

    std::cout << "ok    backfilled " << target->name << "\n";
    return kExitOk;
}

int audit_stream(const Config& cfg, std::uint64_t from_block, std::uint64_t to_block, AuditReport* out_report) {
    namespace net = boost::asio;
    core::log::init("info");

    AuditReport report;
    const auto finish = [&](int code) {
        if (out_report) *out_report = report;
        return code;
    };

    if (from_block > to_block) {
        std::cerr << "FAIL  invalid block range: from_block (" << from_block << ") > to_block (" << to_block << ")\n";
        return finish(kExitUsage);
    }

    const std::string redis_url = core::env::get(cfg.sink.redis_url_env);
    if (redis_url.empty()) {
        std::cerr << "FAIL  environment variable " << cfg.sink.redis_url_env << " is not set\n";
        return finish(kExitFailure);
    }

    auto pool_cfg = make_pool_config(cfg, user_agent(cfg));
    const sink::Keys keys{cfg.shard.id};

    struct FoldedEntry {
        std::string event_id;
        std::string last_type;
        core::StreamKind stream{core::StreamKind::Events};
        std::string finality;
        std::string address;
        std::string topics;
        std::string data;
        std::uint64_t block_number{0};
        std::uint64_t log_index{0};
        bool ever_reverted{false};
    };

    std::map<std::string, FoldedEntry> folded;
    std::vector<std::string> dup_live_samples;
    std::vector<std::string> order_samples;
    std::unordered_map<std::string, std::size_t> source_counts;

    try {
        sink::RedisppConn conn(redis_url, keys);
        const auto ck = conn.read_checkpoint();
        if (!ck.finalized || to_block > *ck.finalized) {
            std::cerr << "FAIL  to_block (" << to_block << ") exceeds ckpt.finalized ("
                      << (ck.finalized ? std::to_string(*ck.finalized) : std::string("none")) << ")\n";
            return finish(kExitUsage);
        }

        sw::redis::ConnectionOptions ropts = sw::redis::Uri(redis_url).connection_options();
        ropts.connect_timeout = std::chrono::seconds(5);
        ropts.socket_timeout = std::chrono::seconds(10);
        sw::redis::Redis redis(ropts);

        using Attrs = std::unordered_map<std::string, std::string>;
        using StreamItem = std::pair<std::string, sw::redis::Optional<Attrs>>;

        for (const auto stream_kind : {core::StreamKind::Events, core::StreamKind::Registry}) {
            std::vector<StreamItem> items;
            redis.xrange(keys.stream(stream_kind), "-", "+", std::back_inserter(items));

            std::map<core::Finality, std::pair<std::uint64_t, std::uint64_t>> last_normal_pos;
            for (const auto& [sid, opt_attrs] : items) {
                (void)sid;
                if (!opt_attrs) continue;
                const auto& attrs = *opt_attrs;
                const auto bn_it = attrs.find("block_number");
                const auto li_it = attrs.find("log_index");
                const auto id_it = attrs.find("event_id");
                const auto ty_it = attrs.find("type");
                const auto fn_it = attrs.find("finality");
                const auto ad_it = attrs.find("address");
                const auto tp_it = attrs.find("topics");
                const auto da_it = attrs.find("data");
                if (bn_it == attrs.end() || li_it == attrs.end() || id_it == attrs.end() || ty_it == attrs.end() ||
                    fn_it == attrs.end() || ad_it == attrs.end() || tp_it == attrs.end() || da_it == attrs.end()) {
                    continue;
                }
                const std::uint64_t bn = std::stoull(bn_it->second);
                if (bn < from_block || bn > to_block) continue;
                const std::uint64_t li = std::stoull(li_it->second);
                const std::string& event_id = id_it->second;
                const std::string& type = ty_it->second;

                if (const auto fs_it = attrs.find("first_source"); fs_it != attrs.end() && !fs_it->second.empty()) {
                    ++source_counts[fs_it->second];
                }

                auto& fe = folded[event_id];
                if (fe.event_id.empty()) fe.event_id = event_id;

                if (type == "log") {
                    if (fe.last_type == "log") {
                        ++report.dup_live;
                        if (dup_live_samples.size() < 20) dup_live_samples.push_back(event_id);
                    } else if (!fe.ever_reverted) {
                        core::Finality fin_class =
                            fn_it->second == "safe" ? core::Finality::Safe : core::Finality::Finalized;
                        bool is_late_correction = false;
                        for (const auto& c : cfg.contracts) {
                            if (c.address.to_hex() == ad_it->second) {
                                if (c.finality == core::Finality::Safe && fn_it->second == "finalized") {
                                    is_late_correction = true;
                                }
                                fin_class = c.finality;
                                break;
                            }
                        }
                        if (!is_late_correction) {
                            const auto pos = std::pair{bn, li};
                            const auto pos_it = last_normal_pos.find(fin_class);
                            if (pos_it != last_normal_pos.end() && pos <= pos_it->second) {
                                ++report.order_violation;
                                if (order_samples.size() < 20) order_samples.push_back(event_id);
                            } else {
                                last_normal_pos[fin_class] = pos;
                            }
                        }
                    }
                    fe.last_type = "log";
                    fe.stream = stream_kind;
                    fe.finality = fn_it->second;
                    fe.address = ad_it->second;
                    fe.topics = tp_it->second;
                    fe.data = da_it->second;
                    fe.block_number = bn;
                    fe.log_index = li;
                } else if (type == "revert") {
                    fe.last_type = "revert";
                    fe.ever_reverted = true;
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "FAIL  redis read failed: " << e.what() << "\n";
        return finish(kExitFailure);
    }

    // Identify the dominant first_source in the audited window and deprioritise it so eth_getLogs uses an independent
    // provider when one is configured and healthy (§6.2).
    std::size_t best_count = 0;
    for (const auto& [src, cnt] : source_counts) {
        if (cnt > best_count || (cnt == best_count && !report.dominant_source.empty() && src < report.dominant_source)) {
            best_count = cnt;
            report.dominant_source = src;
        }
    }
    if (!report.dominant_source.empty() && pool_cfg.getlogs_order.size() > 1) {
        auto it = std::find(pool_cfg.getlogs_order.begin(), pool_cfg.getlogs_order.end(), report.dominant_source);
        if (it != pool_cfg.getlogs_order.end()) {
            pool_cfg.getlogs_order.erase(it);
            pool_cfg.getlogs_order.push_back(report.dominant_source);
        }
    }

    net::io_context ioc;
    auto pool = provider::ProviderPool::create(ioc.get_executor(), pool_cfg, {});
    pool->start_http_only();
    PoolLogSource source(pool, nullptr);

    std::unordered_map<std::string, pipeline::AimdPager> pagers;
    const auto pager_for = [&](const std::string& provider) -> pipeline::AimdPager& {
        const auto it = pagers.find(provider);
        if (it != pagers.end()) return it->second;
        for (const auto& p : cfg.getlogs_chain) {
            if (p.name == provider) {
                return pagers.emplace(provider, pipeline::AimdPager(p.max_getlogs_range, cfg.backfill.min_range))
                    .first->second;
            }
        }
        return pagers.emplace(provider, pipeline::AimdPager(500, cfg.backfill.min_range)).first->second;
    };

    const core::LogFilter filter = cfg.filter();
    std::vector<rpc::Log> canonical_logs;
    bool fetch_ok = true;
    std::string fetch_err;

    net::co_spawn(
        ioc,
        [&]() -> net::awaitable<void> {
            if (filter.addresses.empty()) {
                pool->stop();
                ioc.stop();
                co_return;
            }

            rpc::Backoff backoff(1, std::chrono::milliseconds(100), std::chrono::milliseconds(5000));
            std::uint64_t next_from = from_block;
            while (next_from <= to_block) {
                bool page_done = false;
                for (std::uint32_t attempt = 1; attempt <= 10 && !page_done; ++attempt) {
                    const auto first = source.current_provider();
                    if (!first) {
                        fetch_err = "no usable getLogs provider";
                        break;
                    }
                    auto* pg = &pager_for(*first);
                    for (;;) {
                        const std::uint64_t span_max = pg->range();
                        const std::uint64_t page_to =
                            next_from + std::min<std::uint64_t>(span_max - 1, to_block - next_from);
                        const pipeline::BlockRange r{next_from, page_to};

                        const pipeline::LogResult res = co_await source.get_logs(r, filter);
                        if (res.ok) {
                            if (const auto bad = pipeline::validate_logs(res.logs, r.from, r.to, cfg, std::nullopt)) {
                                source.report_bad(res.provider, *bad);
                                fetch_err = res.provider + ": " + *bad;
                                break;
                            }
                            pg->on_success();
                            if (report.audit_provider.empty()) report.audit_provider = res.provider;
                            canonical_logs.insert(canonical_logs.end(), res.logs.begin(), res.logs.end());
                            next_from = r.to + 1;
                            page_done = true;
                            backoff.reset();
                            break;
                        }

                        if (res.kind != rpc::FailKind::RangeTooLarge) {
                            auto& failed = pager_for(res.provider);
                            const std::uint64_t span = r.to - r.from + 1;
                            if ((res.kind == rpc::FailKind::Retry || res.kind == rpc::FailKind::RateLimited) &&
                                span > failed.min()) {
                                failed.on_range_too_large(std::nullopt);
                            }
                            fetch_err = "getLogs failed on " + res.provider + ": " + res.error;
                            break;
                        }

                        pg = &pager_for(res.provider);
                        if (pg->on_range_too_large(res.range_limit) == pipeline::AimdPager::Shrink::Shrunk) continue;

                        // At minimum range and still too large: split by address.
                        std::vector<rpc::Log> merged;
                        std::string last_provider = res.provider;
                        bool split_ok = true;
                        for (const auto& addr : filter.addresses) {
                            core::LogFilter one{std::vector<core::Address20>{addr}, filter.topic0s};
                            const pipeline::LogResult r2 = co_await source.get_logs(r, one);
                            if (!r2.ok) {
                                fetch_err = "getLogs split failed on " + r2.provider + ": " + r2.error;
                                split_ok = false;
                                break;
                            }
                            if (const auto bad = pipeline::validate_logs(r2.logs, r.from, r.to, cfg, std::nullopt)) {
                                source.report_bad(r2.provider, *bad);
                                fetch_err = r2.provider + ": " + *bad;
                                split_ok = false;
                                break;
                            }
                            last_provider = r2.provider;
                            merged.insert(merged.end(), r2.logs.begin(), r2.logs.end());
                        }
                        if (!split_ok) break;
                        std::sort(merged.begin(), merged.end(), [](const rpc::Log& a, const rpc::Log& b) {
                            return std::tie(a.block_number, a.log_index) < std::tie(b.block_number, b.log_index);
                        });
                        if (report.audit_provider.empty()) report.audit_provider = last_provider;
                        canonical_logs.insert(canonical_logs.end(), merged.begin(), merged.end());
                        next_from = r.to + 1;
                        page_done = true;
                        backoff.reset();
                        break;
                    }
                    if (!page_done && attempt < 10) {
                        net::steady_timer timer(ioc);
                        timer.expires_after(backoff.next());
                        co_await timer.async_wait(net::use_awaitable);
                    }
                }
                if (!page_done) {
                    fetch_ok = false;
                    break;
                }
            }
            pool->stop();
            ioc.stop();
        },
        net::detached);
    ioc.run();

    if (!fetch_ok) {
        std::cerr << "FAIL  audit-stream canonical fetch failed: " << fetch_err << "\n";
        return finish(kExitFailure);
    }

    report.canonical_logs = canonical_logs.size();
    std::unordered_set<std::string> canonical_ids;
    std::vector<std::string> missing_samples;
    std::vector<std::string> phantom_samples;
    std::vector<std::string> mismatch_samples;

    for (const auto& log : canonical_logs) {
        const std::string event_id = pipeline::make_event_id(cfg.chain.chain_id, log.block_hash, log.log_index);
        canonical_ids.insert(event_id);

        const auto it = folded.find(event_id);
        if (it == folded.end() || it->second.last_type != "log") {
            ++report.missing;
            if (missing_samples.size() < 20) missing_samples.push_back(event_id);
            continue;
        }

        const auto& fe = it->second;
        const auto* route = cfg.route(log.address, log.topics.at(0));
        std::string topics_json = "[";
        for (std::size_t i = 0; i < log.topics.size(); ++i) {
            if (i != 0) topics_json += ",";
            topics_json += "\"" + log.topics[i].to_hex() + "\"";
        }
        topics_json += "]";

        const std::string expected_addr = log.address.to_hex();
        const std::string expected_finality = route ? std::string(core::to_string(route->finality)) : std::string{};
        const bool fin_ok = route && (fe.finality == expected_finality ||
                                      (route->finality == core::Finality::Safe && fe.finality == "finalized"));
        const bool stream_ok = route && fe.stream == route->stream;

        if (fe.address != expected_addr || fe.topics != topics_json || fe.data != log.data || !stream_ok || !fin_ok) {
            ++report.field_mismatch;
            if (mismatch_samples.size() < 20) mismatch_samples.push_back(event_id);
        }
    }

    for (const auto& [event_id, fe] : folded) {
        if (fe.last_type != "log") continue;
        ++report.live_stream_entries;
        if (canonical_ids.find(event_id) == canonical_ids.end()) {
            ++report.phantom;
            if (phantom_samples.size() < 20) phantom_samples.push_back(event_id);
        }
    }

    const bool clean =
        report.missing == 0 && report.phantom == 0 && report.dup_live == 0 && report.field_mismatch == 0;
    std::cout << (clean ? "ok    " : "FAIL  ") << "audit-stream blocks=" << from_block << ".." << to_block
              << " provider=" << (report.audit_provider.empty() ? "none" : report.audit_provider)
              << " dominant_source=" << (report.dominant_source.empty() ? "none" : report.dominant_source)
              << " canonical=" << report.canonical_logs << " live=" << report.live_stream_entries
              << " missing=" << report.missing << " phantom=" << report.phantom << " dup_live=" << report.dup_live
              << " field_mismatch=" << report.field_mismatch << " order_violation=" << report.order_violation << "\n";

    const auto print_samples = [](const char* label, const std::vector<std::string>& samples) {
        for (const auto& id : samples) {
            std::cerr << "  " << label << ": " << id << "\n";
        }
    };
    if (!clean) {
        print_samples("missing", missing_samples);
        print_samples("phantom", phantom_samples);
        print_samples("dup_live", dup_live_samples);
        print_samples("field_mismatch", mismatch_samples);
        print_samples("order_violation", order_samples);
        return finish(kExitFailure);
    }
    return finish(kExitOk);
}

/// @brief Command-line interface dispatcher for offline audits and pre-flight probes.
int run_cli(int argc, char** argv) {
    const auto usage = [] {
        std::cerr << "usage: evm_listener --version\n"
                     "       evm_listener --config <file>                        (run the listener)\n"
                     "       evm_listener --config <file> --check-config\n"
                     "       evm_listener --config <file> --check-redis\n"
                     "       evm_listener --config <file> --find-deploy-block <address>\n"
                     "       evm_listener --config <file> --probe-transport <seconds>\n"
                     "       evm_listener --config <file> --backfill-contract <name>\n"
                     "       evm_listener --config <file> --audit-stream <from_block> <to_block>\n";
        return kExitUsage;
    };

    std::optional<std::string> config_path, find_addr, backfill_name;
    std::optional<unsigned> probe_seconds;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> audit_range;
    std::optional<std::chrono::seconds> freeze_io_after;  // test hook, see EVML_TEST_HOOKS
    bool version = false, check = false, check_redis_flag = false;
    // Parse command line arguments.
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--version") {
            version = true;
        } else if (a == "--check-config") {
            check = true;
        } else if (a == "--check-redis") {
            check_redis_flag = true;
        } else if (a == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else if (a == "--find-deploy-block" && i + 1 < argc) {
            find_addr = argv[++i];
        } else if (a == "--backfill-contract" && i + 1 < argc) {
            backfill_name = argv[++i];
            if (backfill_name->empty()) return usage();
        } else if (a == "--audit-stream" && i + 2 < argc) {
            const std::string f_str = argv[++i];
            const std::string t_str = argv[++i];
            const auto is_u64 = [](const std::string& s) {
                return !s.empty() && s.size() <= 20 &&
                       std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
            };
            if (!is_u64(f_str) || !is_u64(t_str)) {
                std::cerr << "--audit-stream needs <from_block> <to_block> as unsigned integers\n";
                return usage();
            }
            audit_range = std::pair{std::stoull(f_str), std::stoull(t_str)};
        } else if (a == "--probe-transport" && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v.empty() || v.size() > 4 || !std::all_of(v.begin(), v.end(), [](unsigned char c) { return std::isdigit(c); })) {
                std::cerr << "--probe-transport needs a number of seconds (1..9999)\n";
                return usage();
            }
            probe_seconds = static_cast<unsigned>(std::stoul(v));
            if (*probe_seconds == 0) return usage();
#ifdef EVML_TEST_HOOKS
        } else if (a == "--test-freeze-io-after" && i + 1 < argc) {
            freeze_io_after = std::chrono::seconds(std::stoul(argv[++i]));
#endif
        } else {
            std::cerr << "unknown or incomplete argument: " << a << "\n";
            return usage();
        }
    }

    // Handle --version flag.
    if (version) {
        if (argc != 2) return usage();
        std::cout << core::version_string() << '\n';
        return kExitOk;
    }
    // The tool actions are mutually exclusive; none of them means "run the listener".
    const int actions = static_cast<int>(check) + static_cast<int>(check_redis_flag) +
                        static_cast<int>(find_addr.has_value()) + static_cast<int>(probe_seconds.has_value()) +
                        static_cast<int>(backfill_name.has_value()) + static_cast<int>(audit_range.has_value());
    if (actions > 1) return usage();
    if (!config_path) {
        std::cerr << "--config <file> is required\n";
        return usage();
    }

    try {
        // Resolve .env files walking parent directories.
        core::env::load_dotenv();
        // Load and validate TOML configuration.
        const auto cfg = Config::load_file(*config_path);
        if (check) {
            print_config_summary(cfg);
            return check_config_online(cfg);
        }
        if (check_redis_flag) return check_redis(cfg);
        if (probe_seconds) return probe_transport(cfg, *probe_seconds);
        if (backfill_name) return backfill_contract(cfg, *backfill_name);
        if (audit_range) return audit_stream(cfg, audit_range->first, audit_range->second);
        if (!find_addr) return run_listener(cfg, freeze_io_after);
        const auto addr = Address20::from_hex(*find_addr);
        std::cout << find_deploy_block(cfg, addr) << "\n";
        return kExitOk;
    } catch (const core::ConfigError& e) {
        std::cerr << "config error: " << e.what() << "\n";
        return kExitUsage;
    } catch (const core::DecodeError& e) {
        std::cerr << "invalid input: " << e.what() << "\n";
        return kExitUsage;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return kExitFailure;
    }
}

}  // namespace evml::app
