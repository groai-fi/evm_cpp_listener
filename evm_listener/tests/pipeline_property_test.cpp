#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>

#include "pipeline/coverage.hpp"
#include "pipeline/pipeline_driver.hpp"
#include "sweep_fakes.hpp"

using namespace evml::pipeline;
using namespace evml::test;

namespace {

constexpr std::size_t kContracts = 3;  // 0, 1: safe class (pools), 2: finalized class (gauge)
constexpr std::uint64_t kStart = 100;
constexpr std::uint64_t kSafeLag = 3;    // g_s = head - kSafeLag
constexpr std::uint64_t kFinalLag = 10;  // g_f = head - kFinalLag; reorgs touch only blocks above g_f

// What Redis does on emit (plan 4.2 emit.lua, rev 5 N1): an entry is accepted only if its type differs from the last
// accepted one for the event_id, and a revert needs a prior log. The in-process LastTypeMap is lost on a restart; this
// is the layer that keeps I6 true across one.
class RedisLikeSink : public MemorySink {
public:
    void emit(Emission e) override {
        const auto it = last_.find(e.event_id);
        if (it == last_.end() ? e.type == EventType::Revert : it->second == e.type) return;
        last_[e.event_id] = e.type;
        MemorySink::emit(std::move(e));
    }

private:
    std::map<std::string, EventType> last_;
};

LogResult retry_failure() {
    LogResult bad;
    bad.provider = "infura";
    bad.kind = evml::rpc::FailKind::Retry;
    bad.error = "boom";
    return bad;
}

LogResult corrupt_failure(const evml::core::Config& cfg) {
    LogResult bad;
    bad.ok = true;
    bad.provider = "infura";
    bad.logs = {mklog(cfg, 0, 999999, 1, 0)};  // out-of-range log -> validate_logs G2 rejection
    return bad;
}

struct Mode {
    bool reorgs{false};
    bool restarts{false};
};

// A scripted chain plus the pipeline under test. Every block has a (block, version) hash: a reorg re-mines the blocks
// with new versions and new random logs, so logs can vanish, appear or move.
class Sim {
public:
    Sim(std::uint64_t seed, Mode mode)
        : cfg_(load_base_config()), mode_(mode), rng_(seed), seed_(seed) {
        if (mode.restarts) sink_ = std::make_unique<RedisLikeSink>();
        else sink_ = std::make_unique<MemorySink>();
        sink_->stored.safe = kStart;
        sink_->stored.finalized = kStart;
        sink_->stored.contracts = contract_manifest(cfg_);
        boot();
    }

    void run_steps(std::size_t n) {
        for (std::size_t i = 0; i < n; ++i) step(i);
    }

    std::size_t reverts() const { return sink_->count(EventType::Revert); }
    std::size_t bad_reports() const { return src_.bad.size(); }

    // Chaos off, gates at the head: everything must settle to exactly the canonical set.
    void converge() {
        INFO("seed " << seed_ << " converge");
        src_.overrides.clear();
        src_.corrupt_next_log_hash = false;
        src_.limit.reset();
        sink_->saturated_flag = false;
        g_s_ = g_f_ = head_;
        bool settled = false;
        for (int i = 0; i < 4000 && !settled; ++i) {
            drv_->set_gates(g_s_, g_f_);
            now_ += 1000;
            const auto t = run(drv_->tick());
            check();
            settled = sw_->c_f() == head_ && sw_->c_s() == head_ && !drv_->status().reverify_pending &&
                      t.wait.count() == 1000;
        }
        const auto st = drv_->status();
        REQUIRE(settled);
        CHECK_FALSE(st.stalled);
        CHECK(net_present(head_) == canonical(head_, false));
    }

private:
    struct Blk {
        std::uint64_t version{};
        std::vector<std::pair<std::size_t, std::uint64_t>> logs;  // (contract index, log index)
    };

    bool roll(std::uint64_t percent) { return rng_() % 100 < percent; }

    static evml::core::Hash32 block_hash(std::uint64_t block, std::uint64_t version) {
        evml::core::Hash32 h;
        h.bytes.fill(0xab);
        for (int i = 0; i < 8; ++i) {
            h.bytes[static_cast<std::size_t>(i)] = static_cast<std::uint8_t>(block >> (8 * i));
            h.bytes[static_cast<std::size_t>(8 + i)] = static_cast<std::uint8_t>(version >> (8 * i));
        }
        return h;
    }

    Blk mine() {
        Blk b;
        b.version = ++versions_;
        const std::uint64_t n = rng_() % 3;
        for (std::uint64_t i = 0; i < n; ++i) b.logs.emplace_back(rng_() % kContracts, i);
        return b;
    }

    void boot() {
        sw_ = std::make_unique<Sweeper>(cfg_, EventContext{"base_eurc_usdc", "test-host", "test"}, src_, *sink_,
                                        [this] { return now_; });
        drv_ = std::make_unique<PipelineDriver>(cfg_, *sw_, *sink_, src_, [this] { return now_; });
    }

    void sync_source() {
        src_.logs.clear();
        src_.blocks.clear();
        for (const auto& [number, blk] : chain_) {
            const auto h = block_hash(number, blk.version);
            src_.blocks[number] = BlockInfo{h, 1700000000 + number};
            for (const auto& [contract, index] : blk.logs) {
                auto l = mklog(cfg_, contract, number, 0, index);
                l.block_hash = h;
                src_.logs.push_back(l);
            }
        }
        dirty_ = false;
    }

    std::string event_id(std::uint64_t number, std::uint64_t index) const {
        return make_event_id(cfg_.chain.chain_id, block_hash(number, chain_.at(number).version), index);
    }

    // Event ids of the canonical logs in blocks <= upto.
    std::set<std::string> canonical(std::uint64_t upto, bool safe_class_only) const {
        std::set<std::string> out;
        for (const auto& [number, blk] : chain_) {
            if (number > upto) break;
            for (const auto& [contract, index] : blk.logs) {
                if (safe_class_only && contract == 2) continue;
                out.insert(event_id(number, index));
            }
        }
        return out;
    }

    // Event ids whose last emitted type is log, restricted to blocks <= upto.
    std::set<std::string> net_present(std::uint64_t upto) const {
        std::set<std::string> out;
        for (const auto& [id, state] : net_)
            if (state.second == EventType::Log && state.first <= upto) out.insert(id);
        return out;
    }

    void advance() {
        const std::uint64_t n = 1 + rng_() % 6;
        for (std::uint64_t i = 0; i < n; ++i) chain_[++head_] = mine();
        dirty_ = true;
    }

    void reorg() {
        const std::uint64_t lowest = std::max(g_f_ + 1, kStart + 1);
        if (head_ < lowest) return;
        const std::uint64_t from = lowest + rng_() % (head_ - lowest + 1);
        for (std::uint64_t b = from; b <= head_; ++b) chain_[b] = mine();
        dirty_ = true;
        // Half the time the host notices (SafeRegression / hash mismatch); otherwise the sweeps must catch it alone.
        if (from <= g_s_ && roll(50)) drv_->on_safe_reorg_signal("test reorg");
    }

    void step(std::size_t i) {
        INFO("seed " << seed_ << " step " << i);
        if (mode_.restarts && roll(3)) boot();
        if (roll(80)) advance();
        g_s_ = std::max(kStart, head_ > kSafeLag ? head_ - kSafeLag : 0);
        g_f_ = std::max(kStart, head_ > kFinalLag ? head_ - kFinalLag : 0);
        if (mode_.reorgs && roll(12)) reorg();
        if (dirty_) sync_source();
        if (roll(10)) src_.overrides.push_back(retry_failure());
        if (roll(5) && src_.overrides.empty()) src_.overrides.push_back(corrupt_failure(cfg_));
        if (roll(8)) src_.corrupt_next_log_hash = true;
        sink_->saturated_flag = roll(10);
        if (roll(5)) {
            static const std::vector<std::optional<std::uint64_t>> limits{std::nullopt, 10, 50, 500};
            src_.limit = limits[rng_() % limits.size()];
        }
        drv_->set_gates(g_s_, g_f_);
        now_ += 1000;
        run(drv_->tick());
        check();
    }

    void check() {
        const auto st = drv_->status();
        REQUIRE(st.state != PipelineState::Failed);
        // I2
        REQUIRE(sw_->c_f() <= sw_->c_s());
        REQUIRE(sw_->c_s() <= g_s_);
        REQUIRE(sw_->c_f() <= g_f_);

        for (; seen_ < sink_->emitted.size(); ++seen_) {
            const auto& e = sink_->emitted[seen_];
            const auto block = static_cast<std::uint64_t>(std::stoull(field(e, "block_number")));
            const auto index = static_cast<std::uint64_t>(std::stoull(field(e, "log_index")));
            // I6: types alternate per event_id, starting with log.
            const auto prev = last_type_.find(e.event_id);
            if (prev == last_type_.end()) REQUIRE(e.type == EventType::Log);
            else REQUIRE(prev->second != e.type);
            last_type_[e.event_id] = e.type;
            net_[e.event_id] = {block, e.type};
            if (e.type != EventType::Log) continue;

            const std::string fin = field(e, "finality");
            // I3: released only within its gate and only if canonical at that moment.
            REQUIRE(block <= (fin == "safe" ? g_s_ : g_f_));
            REQUIRE(field(e, "block_hash") == block_hash(block, chain_.at(block).version).to_hex());
            // I4 (normal release path, so only when nothing reorgs): strictly increasing per class.
            if (!mode_.reorgs) {
                auto& last = fin == "safe" ? last_safe_pos_ : last_final_pos_;
                const auto pos = std::make_pair(block, index);
                if (last) REQUIRE(pos > *last);
                last = pos;
            }
        }

        // I1 (finalized): everything at or below C_f is exactly the canonical set, reorged logs retracted.
        REQUIRE(net_present(sw_->c_f()) == canonical(sw_->c_f(), false));
        // I1 (safe): with no reorgs, every safe-class log at or below C_s has been released.
        if (!mode_.reorgs) {
            const auto want = canonical(sw_->c_s(), true);
            const auto have = net_present(sw_->c_s());
            REQUIRE(std::includes(have.begin(), have.end(), want.begin(), want.end()));
        }
    }

    evml::core::Config cfg_;
    Mode mode_;
    std::mt19937_64 rng_;
    std::uint64_t seed_;
    FakeSource src_;
    std::unique_ptr<MemorySink> sink_;
    std::uint64_t now_{0};
    std::unique_ptr<Sweeper> sw_;
    std::unique_ptr<PipelineDriver> drv_;

    std::map<std::uint64_t, Blk> chain_;
    std::uint64_t head_{kStart};
    std::uint64_t versions_{0};
    std::uint64_t g_s_{kStart}, g_f_{kStart};
    bool dirty_{false};

    std::size_t seen_{0};
    std::map<std::string, EventType> last_type_;
    std::map<std::string, std::pair<std::uint64_t, EventType>> net_;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> last_safe_pos_, last_final_pos_;
};

void run_property(Mode mode, std::uint64_t first_seed, std::uint64_t seeds) {
    std::size_t reverts = 0;
    std::size_t bad_reports = 0;
    for (std::uint64_t seed = first_seed; seed < first_seed + seeds; ++seed) {
        Sim sim(seed, mode);
        sim.run_steps(200);
        sim.converge();
        reverts += sim.reverts();
        bad_reports += sim.bad_reports();
    }
    CHECK(bad_reports > 0);
    // The reorg modes must actually retract logs, otherwise the checks above prove nothing about reorgs.
    if (mode.reorgs) CHECK(reverts > 0);
    else CHECK(reverts == 0);
}

}  // namespace

TEST_CASE("property: without reorgs I1 to I4 and I6 hold under retries, pauses and range limits",
          "[unit][property]") {
    run_property(Mode{false, false}, 1000, 20);  // 20 x 200 ticks
}

TEST_CASE("property: with signalled and silent reorgs I1 to I3 and I6 hold and the emitted set converges",
          "[unit][property]") {
    run_property(Mode{true, false}, 2000, 20);
}

TEST_CASE("property: with reorgs and restarts the persisted state keeps the invariants and converges",
          "[unit][property]") {
    run_property(Mode{true, true}, 3000, 20);
}

// ---- scenarios ----

namespace {

struct Env {
    evml::core::Config cfg = load_base_config();
    FakeSource src;
    RedisLikeSink sink;
    std::uint64_t now{0};
    std::unique_ptr<Sweeper> sw;
    std::unique_ptr<PipelineDriver> drv;

    Env() {
        sink.stored.safe = 100;
        sink.stored.finalized = 100;
        sink.stored.contracts = contract_manifest(cfg);
        boot();
    }
    void boot() {  // a process restart: only the sink survives
        sw = std::make_unique<Sweeper>(cfg, EventContext{"base_eurc_usdc", "test-host", "test"}, src, sink,
                                       [this] { return now; });
        drv = std::make_unique<PipelineDriver>(cfg, *sw, sink, src, [this] { return now; });
    }
    TickResult tick() {
        now += 1000;
        return run(drv->tick());
    }
    void set_chain(const std::vector<std::pair<std::uint64_t, std::uint8_t>>& blocks) {
        src.logs.clear();
        src.blocks.clear();
        for (const auto& [number, h] : blocks) {
            src.logs.push_back(mklog(cfg, 0, number, h, 0));
            src.blocks[number] = binfo(h);
        }
    }
    std::set<std::string> net_present() const {
        std::map<std::string, EventType> last;
        for (const auto& e : sink.emitted) last[e.event_id] = e.type;
        std::set<std::string> out;
        for (const auto& [id, t] : last)
            if (t == EventType::Log) out.insert(id);
        return out;
    }
    std::string id(std::uint64_t, std::uint8_t h) const { return make_event_id(cfg.chain.chain_id, hash_of(h), 0); }
};

}  // namespace

TEST_CASE("scenario: a 3 block reorg below the safe tag is retracted and replaced exactly", "[unit][scenario]") {
    Env env;
    env.set_chain({{105, 1}, {106, 2}, {107, 3}});
    env.drv->set_gates(110, 100);
    env.tick();  // recover
    env.tick();  // safe sweep releases all three
    REQUIRE(env.sink.count(EventType::Log) == 3);

    env.set_chain({{105, 11}, {106, 12}, {107, 13}});
    env.drv->on_safe_reorg_signal("safe tag moved back");
    for (int i = 0; i < 10 && env.drv->status().reverify_pending; ++i) env.tick();

    const auto st = env.drv->status();
    CHECK_FALSE(st.reverify_pending);
    CHECK(st.safe_reorgs == 1);
    CHECK(st.last_fork_point == 104);
    CHECK(env.sink.count(EventType::Revert) == 3);
    CHECK(env.sink.count(EventType::Log) == 6);
    CHECK(env.net_present() == std::set<std::string>{env.id(105, 11), env.id(106, 12), env.id(107, 13)});
    CHECK(env.sw->c_f() == 100);
}

TEST_CASE("scenario: a restart between the safe and final sweeps still retracts from the persisted released set",
          "[unit][scenario]") {
    Env env;
    env.set_chain({{105, 1}});
    env.drv->set_gates(110, 100);
    env.tick();
    env.tick();
    REQUIRE(env.sink.released.size() == 1);

    env.boot();  // crash: in-process state gone, Redis state kept
    env.set_chain({{105, 2}});
    env.drv->set_gates(110, 110);
    for (int i = 0; i < 30 && env.sw->c_f() < 110; ++i) env.tick();

    CHECK(env.sw->c_f() == 110);
    REQUIRE(env.sink.count(EventType::Revert) == 1);
    CHECK(env.net_present() == std::set<std::string>{env.id(105, 2)});
    for (const auto& e : env.sink.emitted)
        if (e.type == EventType::Revert) CHECK(field(e, "block_hash") == hash_of(1).to_hex());  // before image
    CHECK(env.sink.released.empty());
}
