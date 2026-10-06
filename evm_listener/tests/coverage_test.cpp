#include <catch2/catch_test_macros.hpp>

#include <algorithm>

#include "base_config.hpp"
#include "pipeline/coverage.hpp"

using namespace evml::pipeline;
using namespace evml::test;

namespace {
// Manifest without the entry of contract `i` (as if that contract were new).
std::vector<std::string> manifest_without(const evml::core::Config& cfg, std::size_t i) {
    auto m = contract_manifest(cfg);
    const auto prefix = cfg.contracts.at(i).address.to_hex() + ":";
    m.erase(std::remove_if(m.begin(), m.end(), [&](const std::string& e) { return e.rfind(prefix, 0) == 0; }), m.end());
    return m;
}
constexpr std::uint64_t kAfterStart = 52061000;
}  // namespace

TEST_CASE("coverage: the manifest lists address:start:class, sorted, with gate for unset starts", "[unit][coverage]") {
    const auto cfg = load_base_config();
    const auto m = contract_manifest(cfg);
    REQUIRE(m.size() == cfg.contracts.size());
    CHECK(std::is_sorted(m.begin(), m.end()));
    CHECK(std::count(m.begin(), m.end(), cfg.contracts[0].address.to_hex() + ":52060000:safe") == 1);
    CHECK(std::count(m.begin(), m.end(), cfg.contracts[2].address.to_hex() + ":52060000:finalized") == 1);
    CHECK(std::count(m.begin(), m.end(), cfg.contracts[3].address.to_hex() + ":gate:finalized") == 1);
}

TEST_CASE("coverage: a run with its own manifest has nothing uncovered", "[unit][coverage]") {
    const auto cfg = load_base_config();
    CHECK(uncovered_contracts(cfg, contract_manifest(cfg), kAfterStart).empty());
}

TEST_CASE("coverage: a new contract whose history lies below C_s is reported by name", "[unit][coverage]") {
    const auto cfg = load_base_config();
    const auto missing = uncovered_contracts(cfg, manifest_without(cfg, 1), kAfterStart);
    REQUIRE(missing.size() == 1);
    CHECK(missing[0] == cfg.contracts[1].name);
}

TEST_CASE("coverage: a new contract that starts above C_s, or at the gate, needs nothing", "[unit][coverage]") {
    const auto cfg = load_base_config();
    CHECK(uncovered_contracts(cfg, manifest_without(cfg, 1), 52059999).empty());  // C_s below its start
    CHECK(uncovered_contracts(cfg, manifest_without(cfg, 3), kAfterStart).empty());  // registry contract at the gate
    CHECK(uncovered_contracts(cfg, manifest_without(cfg, 4), kAfterStart).empty());
}

TEST_CASE("coverage: moving a start later is covered, moving it earlier is not", "[unit][coverage]") {
    auto cfg = load_base_config();
    const auto stored = contract_manifest(cfg);  // start 52060000

    cfg.contracts[0].start_block = 52060500;
    CHECK(uncovered_contracts(cfg, stored, kAfterStart).empty());

    cfg.contracts[0].start_block = 52059000;
    const auto missing = uncovered_contracts(cfg, stored, kAfterStart);
    REQUIRE(missing.size() == 1);
    CHECK(missing[0] == cfg.contracts[0].name);
}

TEST_CASE("coverage: a contract stored at the gate does not cover a numeric start below C_s", "[unit][coverage]") {
    auto cfg = load_base_config();
    cfg.contracts[3].start_block = std::nullopt;
    const auto stored = contract_manifest(cfg);  // contract 3 stored as gate
    cfg.contracts[3].start_block = 52060100;
    const auto missing = uncovered_contracts(cfg, stored, kAfterStart);
    REQUIRE(missing.size() == 1);
    CHECK(missing[0] == cfg.contracts[3].name);
}

TEST_CASE("coverage: a removed contract needs nothing", "[unit][coverage]") {
    auto cfg = load_base_config();
    const auto stored = contract_manifest(cfg);
    cfg.contracts.erase(cfg.contracts.begin() + 1);
    CHECK(uncovered_contracts(cfg, stored, kAfterStart).empty());
}

TEST_CASE("coverage: a malformed stored manifest fails loudly", "[unit][coverage]") {
    const auto cfg = load_base_config();
    CHECK_THROWS_AS(uncovered_contracts(cfg, {"nonsense"}, kAfterStart), std::runtime_error);
    CHECK_THROWS_AS(uncovered_contracts(cfg, {"0xabc:xx:safe"}, kAfterStart), std::runtime_error);
    CHECK_THROWS_AS(uncovered_contracts(cfg, {":123:safe"}, kAfterStart), std::runtime_error);
}

TEST_CASE("coverage: the first-run cursor is the block before the earliest start, or the gate", "[unit][coverage]") {
    auto cfg = load_base_config();
    CHECK(initial_cursor(cfg, 60000000) == 52059999);

    cfg.contracts[0].start_block = 52050000;
    CHECK(initial_cursor(cfg, 60000000) == 52049999);

    cfg.contracts[0].start_block = 0;
    CHECK(initial_cursor(cfg, 60000000) == 0);  // block 0: no underflow

    cfg.chain.start_policy = evml::core::StartPolicy::Gate;
    CHECK(initial_cursor(cfg, 60000000) == 60000000);
}

TEST_CASE("coverage: contracts at the gate start at the gate when nothing else is earlier", "[unit][coverage]") {
    auto cfg = load_base_config();
    for (auto& c : cfg.contracts) c.start_block = std::nullopt;
    CHECK(initial_cursor(cfg, 60000000) == 60000000);
}
