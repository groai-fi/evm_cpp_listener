#include "app/tools.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <variant>

#include "core/env.hpp"
#include "core/error.hpp"
#include "core/keccak.hpp"
#include "core/version.hpp"
#include "rpc/blocking_http.hpp"
#include "rpc/codec.hpp"

namespace evml::app {

namespace {

using core::Address20;
using core::Config;
using core::Provider;

// A provider answered, but with an error (HTTP non-200 or JSON-RPC error object).
struct RpcFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::string user_agent(const Config& cfg) {
    std::string ua = cfg.ops.http_user_agent;
    const std::string token = "{version}";
    for (auto pos = ua.find(token); pos != std::string::npos; pos = ua.find(token, pos)) {
        ua.replace(pos, token.size(), core::kSemver);
        pos += std::strlen(core::kSemver);
    }
    return ua;
}

std::string clip(std::string_view s, std::size_t n = 120) {
    return std::string(s.substr(0, n));
}

// One JSON-RPC call over HTTP. Returns raw `result` JSON. Throws TransportError, RpcFailure, DecodeError.
std::string rpc_call(const Config& cfg, const Provider& p, const std::string& body, rpc::Decoder& dec) {
    const auto resp = rpc::http_post_json(p.http_url, body, std::chrono::milliseconds(cfg.backfill.request_timeout_ms),
                                          user_agent(cfg));
    if (resp.status != 200) {
        if (const auto err = dec.try_parse_error_body(resp.body)) {
            throw RpcFailure(p.name + ": HTTP " + std::to_string(resp.status) + " rpc error " +
                             std::to_string(err->code) + ": " + clip(err->message));
        }
        throw RpcFailure(p.name + ": HTTP " + std::to_string(resp.status) + ": " + clip(resp.body));
    }
    const auto msg = dec.decode(resp.body);
    const auto* r = std::get_if<rpc::Response>(&msg);
    if (!r) throw RpcFailure(p.name + ": unexpected notification in HTTP response");
    if (!r->ok) throw RpcFailure(p.name + ": rpc error " + std::to_string(r->error.code) + ": " + clip(r->error.message));
    return r->result_raw;
}

bool has_code(const std::string& code_hex) {
    // "0x" = no code; anything longer must be 0x + hex.
    return code_hex.size() > 2;
}

// eth_getCode with failover across getlogs providers. Collects per-provider failures for the final error.
bool code_at(const Config& cfg, const Address20& addr, const std::string& block, rpc::Decoder& dec,
             std::uint64_t id) {
    std::string failures;
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

void print_config_summary(const Config& cfg) {
    std::cout << "shard:    " << cfg.shard.id << " (host " << cfg.shard.host_id << ")\n";
    std::cout << "chain:    " << cfg.chain.name << " id=" << cfg.chain.chain_id << " block_time_ms="
              << cfg.chain.block_time_ms << " start_policy="
              << (cfg.chain.start_policy == core::StartPolicy::Gate ? "gate" : "contract_start") << "\n";
    std::cout << "ws providers:\n";
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

int check_config_online(const Config& cfg) {
    rpc::Decoder dec;
    std::uint64_t id = 1;
    int working = 0;
    bool fatal = false;

    for (const auto& p : cfg.getlogs_chain) {
        try {
            const auto raw = rpc_call(cfg, p, rpc::build_chain_id(id++), dec);
            const auto got = dec.parse_quantity(raw);
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

std::uint64_t find_deploy_block(const Config& cfg, const Address20& address) {
    rpc::Decoder dec;
    std::uint64_t id = 1;

    std::uint64_t head = 0;
    {
        std::string failures;
        bool got = false;
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

    if (!code_at(cfg, address, core::to_hex_u64(head), dec, id++)) {
        throw std::runtime_error(address.to_hex() + " has no code at head block " + std::to_string(head));
    }
    // Invariant: no code at `lo` (block 0 is assumed codeless for any non-genesis contract), code at `hi`.
    std::uint64_t lo = 0, hi = head;
    if (code_at(cfg, address, core::to_hex_u64(lo), dec, id++)) return 0;
    while (hi - lo > 1) {
        const std::uint64_t mid = lo + (hi - lo) / 2;
        if (code_at(cfg, address, core::to_hex_u64(mid), dec, id++)) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    return hi;
}

int run_cli(int argc, char** argv) {
    const auto usage = [] {
        std::cerr << "usage: evm_listener --version\n"
                     "       evm_listener --config <file> --check-config\n"
                     "       evm_listener --config <file> --find-deploy-block <address>\n";
        return kExitUsage;
    };

    std::optional<std::string> config_path, find_addr;
    bool version = false, check = false;
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
        } else {
            std::cerr << "unknown or incomplete argument: " << a << "\n";
            return usage();
        }
    }

    if (version) {
        if (argc != 2) return usage();
        std::cout << core::version_string() << '\n';
        return kExitOk;
    }
    if (check == find_addr.has_value()) return usage();  // need exactly one action
    if (!config_path) {
        std::cerr << "--config <file> is required\n";
        return usage();
    }

    try {
        core::env::load_dotenv();
        const auto cfg = Config::load_file(*config_path);
        if (check) {
            print_config_summary(cfg);
            return check_config_online(cfg);
        }
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
