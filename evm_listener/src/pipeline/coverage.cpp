#include "pipeline/coverage.hpp"

#include <algorithm>
#include <map>
#include <stdexcept>

namespace evml::pipeline {

namespace {
constexpr const char* kGate = "gate";

std::string entry_of(const core::ContractCfg& c) {
    return c.address.to_hex() + ":" + (c.start_block ? std::to_string(*c.start_block) : std::string(kGate)) + ":" +
           core::to_string(c.finality);
}

// "address:start:class" -> {address, start}; start is nullopt for "gate".
struct Stored {
    std::string address;
    std::optional<std::uint64_t> start;
};

Stored parse_entry(const std::string& e) {
    const auto a = e.find(':');
    const auto b = a == std::string::npos ? std::string::npos : e.find(':', a + 1);
    if (a == std::string::npos || b == std::string::npos || a == 0)
        throw std::runtime_error("ckpt.contracts: malformed manifest entry '" + e + "' (expected address:start:class)");
    Stored s;
    s.address = e.substr(0, a);
    const std::string start = e.substr(a + 1, b - a - 1);
    if (start == kGate) return s;
    if (start.empty() || start.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("ckpt.contracts: malformed start_block in manifest entry '" + e + "'");
    s.start = std::stoull(start);
    return s;
}
}  // namespace

std::vector<std::string> contract_manifest(const core::Config& cfg) {
    std::vector<std::string> out;
    out.reserve(cfg.contracts.size());
    for (const auto& c : cfg.contracts) out.push_back(entry_of(c));
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> uncovered_contracts(const core::Config& cfg, const std::vector<std::string>& stored,
                                             std::uint64_t c_s) {
    std::map<std::string, Stored> by_address;
    for (const auto& e : stored) {
        auto s = parse_entry(e);
        by_address[s.address] = std::move(s);
    }

    std::vector<std::string> out;
    for (const auto& c : cfg.contracts) {
        if (!c.start_block || *c.start_block > c_s) continue;  // the sweeps reach it on their own
        const auto it = by_address.find(c.address.to_hex());
        const bool covered = it != by_address.end() && it->second.start && *it->second.start <= *c.start_block;
        if (!covered) out.push_back(c.name);
    }
    return out;
}

std::uint64_t initial_cursor(const core::Config& cfg, std::uint64_t g_s) {
    if (cfg.chain.start_policy == core::StartPolicy::Gate) return g_s;
    std::uint64_t cursor = g_s;
    for (const auto& c : cfg.contracts) {
        if (!c.start_block) continue;
        cursor = std::min<std::uint64_t>(cursor, *c.start_block > 0 ? *c.start_block - 1 : 0);
    }
    return cursor;
}

}  // namespace evml::pipeline
