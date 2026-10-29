#include <catch2/catch_test_macros.hpp>

#include "base_config.hpp"
#include "chain/finality_gate.hpp"

using namespace evml::chain;
using evml::test::hash_of;

TEST_CASE("gate: monotonic progress reports no signal and tracks the latest values", "[unit][gate]") {
    FinalityGate g;
    CHECK_FALSE(g.safe().has_value());
    CHECK_FALSE(g.finalized().has_value());
    CHECK(g.on_safe("infura", 100, hash_of(1)) == GateSignal::None);
    CHECK(g.on_safe("infura", 105, hash_of(2)) == GateSignal::None);
    CHECK(g.on_finalized("infura", 90, hash_of(3)) == GateSignal::None);
    CHECK(g.on_finalized("infura", 95, hash_of(4)) == GateSignal::None);
    CHECK(g.safe() == 105u);
    CHECK(g.finalized() == 95u);
    CHECK_FALSE(g.frozen());
}

TEST_CASE("gate: finalized is clamped to safe (I2: G_f <= G_s)", "[unit][gate]") {
    FinalityGate g;
    g.on_finalized("infura", 200, hash_of(1));
    CHECK(g.finalized() == 200u);  // no safe observation yet
    g.on_safe("infura", 150, hash_of(2));
    CHECK(g.finalized() == 150u);
}

TEST_CASE("gate: a safe regression from the SAME provider is a safe-reorg signal, and the gate follows it", "[unit][gate]") {
    FinalityGate g;
    g.on_safe("infura", 100, hash_of(1));
    CHECK(g.on_safe("infura", 98, hash_of(2)) == GateSignal::SafeRegression);
    CHECK(g.safe() == 98u);
    CHECK(g.on_safe("infura", 99, hash_of(3)) == GateSignal::None);  // recovered: no repeated signal
    CHECK_FALSE(g.frozen());                                          // a safe reorg is not a freeze
}

TEST_CASE("gate: same safe number with a different hash is a safe-reorg signal", "[unit][gate]") {
    FinalityGate g;
    g.on_safe("infura", 100, hash_of(1));
    CHECK(g.on_safe("infura", 100, hash_of(1)) == GateSignal::None);
    CHECK(g.on_safe("infura", 100, hash_of(9)) == GateSignal::SafeHashMismatch);
}

TEST_CASE("gate: a lower tag from a DIFFERENT provider (getLogs failover) is not an anomaly", "[unit][gate]") {
    FinalityGate g;
    g.on_safe("infura", 100, hash_of(1));
    g.on_finalized("infura", 90, hash_of(2));
    CHECK(g.on_safe("ankr", 80, hash_of(3)) == GateSignal::None);
    CHECK(g.on_finalized("ankr", 70, hash_of(4)) == GateSignal::None);
    CHECK_FALSE(g.frozen());
    CHECK(g.safe() == 80u);  // the sweeps simply wait until the tag passes their cursors
    CHECK(g.finalized() == 70u);
    // ...but going back to the first provider, its own history still applies.
    CHECK(g.on_safe("infura", 95, hash_of(5)) == GateSignal::SafeRegression);
}

TEST_CASE("gate: finalized ignores load-balanced stale heights and freezes on same-height hash conflict", "[unit][gate]") {
    SECTION("stale lower height from load-balanced node is ignored (monotonic G_f)") {
        FinalityGate g;
        g.on_finalized("infura", 90, hash_of(1));
        CHECK(g.on_finalized("infura", 89, hash_of(2)) == GateSignal::None);
        CHECK_FALSE(g.frozen());
        CHECK(g.finalized() == 90u);
        // Stored height 90 hash is preserved: a conflicting hash at 90 still freezes release.
        CHECK(g.on_finalized("infura", 90, hash_of(2)) == GateSignal::FinalizedAnomaly);
        CHECK(g.frozen());
    }
    SECTION("same number, different hash") {
        FinalityGate g;
        g.on_finalized("infura", 90, hash_of(1));
        CHECK(g.on_finalized("infura", 90, hash_of(2)) == GateSignal::FinalizedAnomaly);
        CHECK(g.frozen());
        CHECK(g.frozen_reason().find("infura") != std::string::npos);
        CHECK(g.frozen_reason().find("changed hash") != std::string::npos);
        g.on_finalized("infura", 120, hash_of(3));  // recovering does not unfreeze: restart required
        CHECK(g.frozen());
    }
    SECTION("cross-provider same height hash mismatch") {
        FinalityGate g;
        CHECK(g.on_safe("infura", 100, hash_of(1)) == GateSignal::None);
        CHECK(g.on_safe("ankr", 100, hash_of(1)) == GateSignal::None);
        CHECK(g.on_safe("alchemy", 100, hash_of(2)) == GateSignal::SafeHashMismatch);
        CHECK_FALSE(g.frozen());

        CHECK(g.on_finalized("infura", 90, hash_of(3)) == GateSignal::None);
        CHECK(g.on_finalized("ankr", 90, hash_of(3)) == GateSignal::None);
        CHECK(g.on_finalized("alchemy", 90, hash_of(4)) == GateSignal::FinalizedAnomaly);
        CHECK(g.frozen());
        CHECK(g.frozen_reason().find("disagrees with") != std::string::npos);
    }
}
