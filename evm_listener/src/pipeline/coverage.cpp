#include "pipeline/coverage.hpp"

#include <algorithm>
#include <charconv>
#include <map>
#include <stdexcept>
#include <string_view>

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

Stored parse_entry(std::string_view e) {
    const auto a = e.find(':');
    const auto b = a == std::string_view::npos ? std::string_view::npos : e.find(':', a + 1);
    if (a == std::string_view::npos || b == std::string_view::npos || a == 0)
        throw std::runtime_error("ckpt.contracts: malformed manifest entry '" + std::string(e) +
                                 "' (expected address:start:class)");
    Stored s;
    s.address = std::string(e.substr(0, a));
    const std::string_view start = e.substr(a + 1, b - a - 1);
    if (start == kGate) return s;
    if (start.empty() || start.find_first_not_of("0123456789") != std::string_view::npos)
        throw std::runtime_error("ckpt.contracts: malformed start_block in manifest entry '" + std::string(e) + "'");
    std::uint64_t val = 0;
    const auto [ptr, ec] = std::from_chars(start.data(), start.data() + start.size(), val);
    if (ec != std::errc{} || ptr != start.data() + start.size())
        throw std::runtime_error("ckpt.contracts: malformed start_block in manifest entry '" + std::string(e) + "'");
    s.start = val;
    return s;
}
}  // namespace

// Builds lexicographically sorted list of "address:start_block:class" strings.
std::vector<std::string> contract_manifest(const core::Config& cfg) {
    std::vector<std::string> out;
    out.reserve(cfg.contracts.size());
    for (const auto& c : cfg.contracts)
        out.push_back(entry_of(c));
    std::sort(out.begin(), out.end());  // Deterministic sorting ensures exact equality comparisons in Redis
    return out;
}

// Scans stored Redis manifest to ensure every contract with start_block <= C_s was covered in prior runs.
std::vector<std::string> uncovered_contracts(const core::Config& cfg, const std::vector<std::string>& stored,
                                             std::uint64_t c_s) {
    std::map<std::string, Stored> by_address;
    for (const auto& e : stored) {
        auto s = parse_entry(e);
        by_address[s.address] = std::move(s);
    }

    std::vector<std::string> out;
    for (const auto& c : cfg.contracts) {
        // Contracts starting above C_s or at 'gate' will naturally be reached by live sweeps; skip check
        if (!c.start_block || *c.start_block > c_s) continue;  // the sweeps reach it on their own
        const auto it = by_address.find(c.address.to_hex());
        // Verify historical presence: must exist in stored manifest with start_block <= current config start
        const bool covered = it != by_address.end() && it->second.start && *it->second.start <= *c.start_block;
        if (!covered) out.push_back(c.name);  // Missing coverage below C_s
    }
    return out;
}

// Determines the initial sweep start cursor; returns lowest start_block - 1 or gate G_s.
std::uint64_t initial_cursor(const core::Config& cfg, std::uint64_t g_s) noexcept {
    if (cfg.chain.start_policy == core::StartPolicy::Gate) return g_s;
    std::uint64_t cursor = g_s;
    for (const auto& c : cfg.contracts) {
        if (!c.start_block) continue;
        // Invariant I1: sweep begins at cursor + 1, so cursor must be start_block - 1
        cursor = std::min<std::uint64_t>(cursor, *c.start_block > 0 ? *c.start_block - 1 : 0);
    }
    return cursor;
}

}  // namespace evml::pipeline
