#include <catch2/catch_test_macros.hpp>

#include <string>

#include "core/error.hpp"
#include "core/keccak.hpp"

using namespace evml::core;

namespace {
struct Vector {
    const char* signature;
    const char* topic0;
};
// Full values computed independently with `cast keccak`; Swap/Deposit/Withdraw/ClaimRewards/NotifyReward/
// ClaimFees additionally match topic0s observed in live Base logs (see 02_dynamic_discovery_review).
// The registry events (PoolCreated, GaugeCreated/Killed/Revived) are NOT yet live-confirmed.
constexpr Vector kVectors[] = {
    {"Transfer(address,address,uint256)", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
    {"Swap(address,address,int256,int256,uint160,uint128,int24)",
     "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"},
    {"Mint(address,address,int24,int24,uint128,uint256,uint256)",
     "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"},
    {"Burn(address,int24,int24,uint128,uint256,uint256)",
     "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"},
    {"Collect(address,address,int24,int24,uint128,uint128)",
     "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"},
    {"Deposit(address,uint256,uint128)", "0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211"},
    {"Withdraw(address,uint256,uint128)", "0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84"},
    {"ClaimRewards(address,uint256)", "0x1f89f96333d3133000ee447473151fa9606543368f02271c9d95ae14f13bcc67"},
    {"NotifyReward(address,uint256)", "0x095667752957714306e1a6ad83495404412df6fdb932fca6dc849a7ee910d4c1"},
    {"ClaimFees(address,uint256,uint256)", "0xbc567d6cbad26368064baa0ab5a757be46aae4d70f707f9203d9d9b6c8ccbfa3"},
    {"PoolCreated(address,address,int24,address)",
     "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2"},
    {"GaugeCreated(address,address,address,address,address,address,address,address)",
     "0xef9f7d1ffff3b249c6b9bf2528499e935f7d96bb6d6ec4e7da504d1d3c6279e1"},
    {"GaugeKilled(address)", "0x04a5d3f5d80d22d9345acc80618f4a4e7e663cf9e1aed23b57d975acec002ba7"},
    {"GaugeRevived(address)", "0xed18e9faa3dccfd8aa45f69c4de40546b2ca9cccc4538a2323531656516db1aa"},
};
}  // namespace

TEST_CASE("topic0 matches independently computed vectors for every v1 event", "[unit][keccak]") {
    for (const auto& v : kVectors) {
        INFO(v.signature);
        CHECK(topic0(v.signature).to_hex() == v.topic0);
    }
}

TEST_CASE("validate_event_signature accepts canonical forms", "[unit][keccak]") {
    CHECK_NOTHROW(validate_event_signature("Foo()"));
    CHECK_NOTHROW(validate_event_signature("Foo(uint256[],(address,uint8)[])"));
    CHECK_NOTHROW(validate_event_signature("Foo_1(bytes32)"));
}

TEST_CASE("validate_event_signature rejects non-canonical forms", "[unit][keccak]") {
    CHECK_THROWS_AS(topic0(""), ConfigError);
    CHECK_THROWS_AS(topic0("Transfer(address from,address to,uint256 value)"), ConfigError);  // named params
    CHECK_THROWS_AS(topic0("Transfer(address, address)"), ConfigError);                       // space
    CHECK_THROWS_AS(topic0("Transfer"), ConfigError);                                         // no parens
    CHECK_THROWS_AS(topic0("(address)"), ConfigError);                                        // no name
    CHECK_THROWS_AS(topic0("Transfer(address"), ConfigError);                                 // unbalanced
    CHECK_THROWS_AS(topic0("Transfer)address("), ConfigError);
    CHECK_THROWS_AS(topic0("Transfer(address,,uint256)"), ConfigError);                       // empty component
    CHECK_THROWS_AS(topic0("Transfer(address,)"), ConfigError);
    CHECK_THROWS_AS(topic0("Transfer(address)x"), ConfigError);                               // trailing junk
    CHECK_THROWS_AS(topic0("Transfer(address);drop"), ConfigError);
}
