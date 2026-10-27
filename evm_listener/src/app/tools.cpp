#include "app/tools.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <variant>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "app/host.hpp"
#include "app/pool_config.hpp"
#include "core/env.hpp"
#include "core/error.hpp"
#include "core/keccak.hpp"
#include "core/log.hpp"
#include "core/version.hpp"
#include "provider/provider_pool.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/codec.hpp"

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
    h.on_ws_active_changed = [](const std::optional<std::string>& p, std::string_view why) {
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

/// @brief Command-line interface dispatcher for offline audits and pre-flight probes.
int run_cli(int argc, char** argv) {
    const auto usage = [] {
        std::cerr << "usage: evm_listener --version\n"
                     "       evm_listener --config <file>                        (run the listener)\n"
                     "       evm_listener --config <file> --check-config\n"
                     "       evm_listener --config <file> --find-deploy-block <address>\n"
                     "       evm_listener --config <file> --probe-transport <seconds>\n";
        return kExitUsage;
    };

    std::optional<std::string> config_path, find_addr;
    std::optional<unsigned> probe_seconds;
    std::optional<std::chrono::seconds> freeze_io_after;  // test hook, see EVML_TEST_HOOKS
    bool version = false, check = false;
    // Parse command line arguments.
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--version") {
            version = true;
        } else if (a == "--check-config") {
            check = true;
        } else if (a == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else if (a == "--find-deploy-block" && i + 1 < argc) {
            find_addr = argv[++i];
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
    // The three tool actions are mutually exclusive; none of them means "run the listener".
    const int actions = static_cast<int>(check) + static_cast<int>(find_addr.has_value()) + static_cast<int>(probe_seconds.has_value());
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
        if (probe_seconds) return probe_transport(cfg, *probe_seconds);
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
