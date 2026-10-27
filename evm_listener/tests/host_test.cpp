// Host tests (plan 60 §5.H, slice 5-D1). The in-process tests run the real host (watchdog, ops server, sink, pool,
// sweeper, driver) against the compose Redis (REDIS_URL, process environment only) and an in-process mock JSON-RPC
// server. Each test uses its own random shard id and removes its keys afterwards.
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sw/redis++/redis++.h>

#include "app/host.hpp"
#include "app/tools.hpp"
#include "base_config.hpp"
#include "core/config.hpp"
#include "core/hex.hpp"
#include "core/log.hpp"
#include "mock_http_server.hpp"
#include "sink/keys.hpp"
#include "sink/lease.hpp"
#include "sink/lua_scripts.hpp"
#include "sink/script_runner.hpp"
#include "test_util.hpp"
#include "toxiproxy.hpp"

#if defined(EVML_TEST_HOOKS)
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

using namespace std::chrono_literals;
namespace net = boost::asio;
namespace fs = std::filesystem;
using evml::testing::require_env;

namespace {

std::string random_tag() {
    std::random_device rd;
    return std::to_string(rd()) + std::to_string(rd());
}

std::string read_base_toml() {
    std::ifstream f(std::string(EVML_CONFIG_DIR) + "/base.toml", std::ios::binary);
    REQUIRE(f.good());
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

void replace_once(std::string& text, const std::string& from, const std::string& to) {
    const auto pos = text.find(from);
    INFO("base.toml no longer contains: " << from);
    REQUIRE(pos != std::string::npos);
    text.replace(pos, from.size(), to);
}

// base.toml adapted for a run on this machine: lease off unless `lease_enabled`, private shard and spill dir, loopback
// ops port, fast tag polls, and the credential-free public provider URL pointed at `http_url`.
std::string host_toml(const std::string& shard, const fs::path& spill, const std::string& http_url,
                      std::uint32_t process_stall_ms, bool lease_enabled = false, std::uint32_t ttl_ms = 3000,
                      std::uint32_t renew_ms = 500) {
    auto t = read_base_toml();
    replace_once(t, "id      = \"base_eurc_usdc\"", "id      = \"" + shard + "\"");
    if (lease_enabled) {
        replace_once(t, "ttl_ms   = 15000", "ttl_ms   = " + std::to_string(ttl_ms));
        replace_once(t, "renew_ms = 5000", "renew_ms = " + std::to_string(renew_ms));
    } else {
        replace_once(t, "enabled  = true", "enabled  = false");
    }
    replace_once(t, "safe_poll_ms       = 4000", "safe_poll_ms       = 100");
    replace_once(t, "finalized_poll_ms  = 12000", "finalized_poll_ms  = 100");
    replace_once(t, "process_stall_ms     = 30000", "process_stall_ms     = " + std::to_string(process_stall_ms));
    replace_once(t, "spill_dir          = \"/tmp/evml-spill\"", "spill_dir          = \"" + spill.string() + "\"");
    replace_once(t, "http_listen             = \"[::]:9464\"", "http_listen             = \"127.0.0.1:0\"");
    replace_once(t, "\"https://mainnet.base.org\"", "\"" + http_url + "\"");
    return t;
}

// Every provider env var of base.toml: HTTP -> `http_url`, WS -> a port nobody listens on (validated, never served).
evml::core::EnvLookup provider_env(const std::string& http_url, const std::string& host_id = "test-host") {
    return [http_url, host_id](const std::string& k) -> std::string {
        if (k.rfind("BASE_RPC_URL", 0) == 0) return http_url;
        if (k.rfind("BASE_WSS_URL", 0) == 0) return "ws://127.0.0.1:9/";
        if (k == "EVML_HOST_ID") return host_id;
        return "";
    };
}

std::uint64_t first_start_block(const evml::core::Config& cfg) {
    std::uint64_t lo = UINT64_MAX;
    for (const auto& c : cfg.contracts) {
        if (c.start_block) lo = std::min(lo, *c.start_block);
    }
    REQUIRE(lo != UINT64_MAX);
    return lo;
}

// Removes the test's shard keys and spill dir on scope exit.
struct Cleanup {
    std::string shard;
    fs::path spill;
    ~Cleanup() {
        try {
            sw::redis::Redis admin{require_env("REDIS_URL")};
            const evml::sink::Keys keys{shard};
            std::vector<std::string> mine;
            admin.keys(keys.pattern(), std::back_inserter(mine));
            if (!mine.empty()) admin.del(mine.begin(), mine.end());
        } catch (const std::exception& e) {
            std::cerr << "host test cleanup of shard keys failed: " << e.what() << "\n";
        }
        std::error_code ec;
        fs::remove_all(spill, ec);
    }
};

std::string rpc_reply(std::uint64_t id, const std::string& result_json) {
    return R"({"jsonrpc":"2.0","id":)" + std::to_string(id) + R"(,"result":)" + result_json + "}";
}

std::string block_json(std::uint64_t n) {
    return R"({"number":")" + evml::core::to_hex_u64(n) + R"(","hash":"0x)" + std::string(64, 'a') +
           R"(","parentHash":"0x)" + std::string(64, 'b') + R"(","timestamp":"0x1"})";
}

// A healthy node a few hundred blocks above the contracts' start block; unknown methods fail loudly (HTTP 500).
evml::test::MockHttpServer::Handler healthy_node(std::uint64_t safe, std::uint64_t finalized) {
    return [=](const evml::test::MockRequest& r) {
        const auto v = boost::json::parse(r.body).as_object();
        const std::string method(v.at("method").as_string());
        const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
        if (method == "eth_chainId") return evml::test::MockReply{.body = rpc_reply(id, "\"0x2105\"")};
        if (method == "eth_blockNumber") {
            return evml::test::MockReply{.body = rpc_reply(id, "\"" + evml::core::to_hex_u64(safe + 10) + "\"")};
        }
        if (method == "eth_getBlockByNumber") {
            const std::string tag(v.at("params").as_array().at(0).as_string());
            const auto n = tag == "safe" ? safe : tag == "finalized" ? finalized : safe + 10;
            return evml::test::MockReply{.body = rpc_reply(id, block_json(n))};
        }
        if (method == "eth_getLogs") return evml::test::MockReply{.body = rpc_reply(id, "[]")};
        return evml::test::MockReply{.status = 500, .body = "unexpected method " + method};
    };
}

// A node whose tags and health the test changes between run_until calls. The mock server runs on the test thread, so
// the handler and the test never touch `NodeState` concurrently.
struct NodeState {
    std::uint64_t safe{0};
    std::uint64_t finalized{0};
    bool fail{false};  // every request answers HTTP 500
    int getlogs_calls{0};
};

evml::test::MockHttpServer::Handler tunable_node(NodeState& s) {
    return [&s](const evml::test::MockRequest& r) {
        if (s.fail) return evml::test::MockReply{.status = 500, .body = "node down"};
        if (r.body.find("eth_getLogs") != std::string::npos) ++s.getlogs_calls;
        return healthy_node(s.safe, s.finalized)(r);
    };
}

// One host (run_host on its own thread) with its observable state.
struct RunningHost {
    evml::core::Config cfg;
    std::atomic<bool> stop{false};
    std::atomic<int> state{-1};
    std::atomic<int> exit_code{-1};
    std::atomic<int> leader_events{0};
    std::atomic<int> demoted_events{0};
    std::atomic<bool> threw{false};
    std::string what;
    std::mutex mu;
    evml::pipeline::DriverStatus last;  // guarded by `mu`
    std::thread th;

    explicit RunningHost(evml::core::Config c) : cfg(std::move(c)) {}
    void start() {
        th = std::thread([this] {
            evml::app::HostOptions opt;
            opt.stop = &stop;
            opt.beat_period = 50ms;
            opt.on_status = [this](const evml::pipeline::DriverStatus& s) {
                state = static_cast<int>(s.state);
                std::lock_guard<std::mutex> lk(mu);
                last = s;
            };
            opt.on_event = [this](std::string_view e) {
                if (e == "leader") ++leader_events;
                if (e == "demoted") {
                    ++demoted_events;
                    state = -1;
                }
            };
            try {
                exit_code = evml::app::run_host(cfg, opt);
            } catch (const std::exception& e) {
                what = e.what();
                threw = true;
            }
        });
    }
    bool live() const { return state.load() == static_cast<int>(evml::pipeline::PipelineState::Live); }
    evml::pipeline::DriverStatus status() {
        std::lock_guard<std::mutex> lk(mu);
        return last;
    }
    void finish() {
        stop = true;
        if (th.joinable()) th.join();
    }
    ~RunningHost() { finish(); }
};

}  // namespace

TEST_CASE("host: cold start reaches Live, writes the initial checkpoint, and a stop request exits 0", "[integration]") {
    evml::core::log::init("info");
    const std::string redis_url = require_env("REDIS_URL");  // the host reads it from the same env var
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-it-" + random_tag());
    Cleanup cleanup{shard, spill};

    // Mock node: safe/finalized sit 300/250 blocks above the start block, inside one page (live_lag_blocks = 500).
    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    const auto cfg = evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000), "host_test.toml",
                                               provider_env(node.url()));

    std::atomic<bool> stop{false};
    std::atomic<int> state{-1};
    std::atomic<int> exit_code{-1};
    std::atomic<bool> threw{false};
    std::string what;
    evml::app::HostOptions opt;
    opt.stop = &stop;
    opt.beat_period = 50ms;
    opt.on_status = [&](const evml::pipeline::DriverStatus& s) { state = static_cast<int>(s.state); };
    std::thread host([&] {
        try {
            exit_code = evml::app::run_host(cfg, opt);
        } catch (const std::exception& e) {
            what = e.what();
            threw = true;
        }
    });

    const bool live = evml::test::run_until(
        mock_ioc, [&] { return state.load() == static_cast<int>(evml::pipeline::PipelineState::Live) || threw.load(); }, 30s);
    stop = true;
    host.join();
    INFO("host threw: " << what);
    REQUIRE_FALSE(threw.load());
    REQUIRE(live);
    CHECK(exit_code.load() == 0);

    // The first run wrote the initial checkpoint at c0 for both cursors (one atomic write, plan 3.2).
    sw::redis::Redis admin{redis_url};
    const evml::sink::Keys keys{shard};
    std::unordered_map<std::string, std::string> ckpt;
    admin.hgetall(keys.ckpt(), std::inserter(ckpt, ckpt.begin()));
    REQUIRE_FALSE(ckpt.empty());
    CHECK(std::stoull(ckpt.at("safe")) >= c0);
    CHECK(std::stoull(ckpt.at("finalized")) >= c0);
}

TEST_CASE("host: a standby takes over within the lease ttl when the leader stops, without moving the checkpoint back",
          "[integration]") {
    evml::core::log::init("info");
    const std::string redis_url = require_env("REDIS_URL");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill_a = fs::temp_directory_path() / ("evml-host-a-" + random_tag());
    const fs::path spill_b = fs::temp_directory_path() / ("evml-host-b-" + random_tag());
    Cleanup cleanup{shard, spill_a};
    Cleanup cleanup_b{shard, spill_b};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    RunningHost a(evml::core::Config::parse(host_toml(shard, spill_a, node.url(), 30000, true), "a.toml",
                                            provider_env(node.url(), "hostA")));
    RunningHost b(evml::core::Config::parse(host_toml(shard, spill_b, node.url(), 30000, true), "b.toml",
                                            provider_env(node.url(), "hostB")));

    a.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return a.live() || a.threw.load(); }, 30s));
    REQUIRE_FALSE(a.threw.load());

    b.start();
    evml::test::run_until(mock_ioc, [&] { return b.live() || b.threw.load(); }, 1500ms);  // must time out
    CHECK_FALSE(b.live());
    CHECK(b.leader_events.load() == 0);

    sw::redis::Redis admin{redis_url};
    const evml::sink::Keys keys{shard};
    const auto holder = admin.get(keys.leader());
    REQUIRE(holder.has_value());
    CHECK(holder->find("hostA") != std::string::npos);
    std::unordered_map<std::string, std::string> before;
    admin.hgetall(keys.ckpt(), std::inserter(before, before.begin()));
    REQUIRE_FALSE(before.empty());

    a.finish();
    CHECK(a.exit_code.load() == 0);
    const bool took_over = evml::test::run_until(mock_ioc, [&] { return b.live() || b.threw.load(); }, 15s);
    INFO("host B threw: " << b.what);
    REQUIRE_FALSE(b.threw.load());
    REQUIRE(took_over);
    CHECK(b.leader_events.load() == 1);

    std::unordered_map<std::string, std::string> after;
    admin.hgetall(keys.ckpt(), std::inserter(after, after.begin()));
    CHECK(std::stoull(after.at("safe")) >= std::stoull(before.at("safe")));
    CHECK(std::stoull(after.at("finalized")) >= std::stoull(before.at("finalized")));

    b.finish();
    CHECK(b.exit_code.load() == 0);
}

TEST_CASE("host: losing the lease demotes the host to standby and it leads again after the intruder expires",
          "[integration]") {
    evml::core::log::init("info");
    const std::string redis_url = require_env("REDIS_URL");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-fence-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    RunningHost h(evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000, true), "fence.toml",
                                            provider_env(node.url(), "hostA")));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // Steal the lease: delete the leader key and acquire it as another host.
    sw::redis::Redis admin{redis_url};
    const evml::sink::Keys keys{shard};
    admin.del(keys.leader());
    evml::sink::ScriptRunner runner(admin);
    REQUIRE(runner.run(evml::sink::lease_acquire_call(keys, "intruder", "n1", 3000)).has_value());

    const bool demoted = evml::test::run_until(
        mock_ioc, [&] { return h.demoted_events.load() >= 1 || h.threw.load(); }, 15s);
    INFO("host threw: " << h.what);
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(demoted);

    const bool relead = evml::test::run_until(
        mock_ioc, [&] { return (h.leader_events.load() >= 2 && h.live()) || h.threw.load(); }, 30s);
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(relead);
    CHECK(h.demoted_events.load() == 1);

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

// ---- 5-D3: the gate bridge (on_tag -> FinalityGate -> driver) as seen from the host ----

TEST_CASE("host: a finalized regression on the current provider freezes release and stops the sweeps", "[integration]") {
    evml::core::log::init("info");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-frozen-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns{.safe = c0 + 300, .finalized = c0 + 250};
    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
    RunningHost h(evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000), "frozen.toml",
                                            provider_env(node.url())));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());
    CHECK_FALSE(h.status().frozen);

    ns.finalized = c0 + 100;  // an impossible regression of the finalized tag
    const bool frozen = evml::test::run_until(mock_ioc, [&] { return h.status().frozen || h.threw.load(); }, 15s);
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(frozen);
    const auto st = h.status();
    CHECK(st.frozen_reason.find("finalized regressed") != std::string::npos);
    CHECK(st.c_s == c0 + 300);  // frozen: the cursors did not move
    CHECK(st.c_f == c0 + 250);

    // Frozen means no more sweeps: even a rising safe gate does not start a getLogs call.
    ns.safe = c0 + 320;
    const int calls = ns.getlogs_calls;
    evml::test::run_until(mock_ioc, [] { return false; }, 1500ms);
    CHECK(ns.getlogs_calls == calls);
    CHECK(h.status().c_s == c0 + 300);

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

TEST_CASE("host: a safe regression on the current provider makes the driver re-verify the released window",
          "[integration]") {
    evml::core::log::init("info");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-safereorg-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns{.safe = c0 + 300, .finalized = c0 + 250};
    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
    RunningHost h(evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000), "safereorg.toml",
                                            provider_env(node.url())));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // Quiet baseline: Live with C_f < C_s and nothing left to sweep, so no getLogs calls happen on their own.
    evml::test::run_until(mock_ioc, [] { return false; }, 700ms);
    const int before = ns.getlogs_calls;
    evml::test::run_until(mock_ioc, [] { return false; }, 700ms);
    REQUIRE(ns.getlogs_calls == before);

    ns.safe = c0 + 290;  // the same provider's safe tag goes backwards: possible L1 reorg
    const bool reverified =
        evml::test::run_until(mock_ioc, [&] { return ns.getlogs_calls > before || h.threw.load(); }, 15s);
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(reverified);  // reverify_released fetched (C_f, C_s] again

    const bool settled = evml::test::run_until(
        mock_ioc, [&] { return !h.status().reverify_pending && h.live(); }, 15s);
    REQUIRE(settled);
    const auto st = h.status();
    CHECK_FALSE(st.frozen);
    CHECK(st.safe_reorgs == 0);  // the mock returns the same (empty) logs: nothing differed
    CHECK(st.g_s == c0 + 290);

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

TEST_CASE("host: failing over to a provider with lower tags neither freezes nor re-verifies", "[integration]") {
    evml::core::log::init("info");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-failover-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState primary{.safe = c0 + 300, .finalized = c0 + 250};
    NodeState other{.safe = c0 + 280, .finalized = c0 + 230};  // a lagging, but honest, node
    evml::test::MockHttpServer node_a(mock_ioc, tunable_node(primary));
    evml::test::MockHttpServer node_b(mock_ioc, tunable_node(other));
    // Every env provider (infura, alchemy, ankr, quicknode) talks to node A. host_toml points base_public (the
    // credential-free provider, 4th in getlogs_order) at node B, so it is the one that leads after A dies.
    RunningHost h(evml::core::Config::parse(host_toml(shard, spill, node_b.url(), 30000), "failover.toml",
                                            provider_env(node_a.url())));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());
    CHECK(h.status().g_s == c0 + 300);

    primary.fail = true;  // A dies: infura, alchemy and ankr each fail 3 times in a row, then base_public leads
    const bool moved = evml::test::run_until(
        mock_ioc, [&] { return h.status().g_s == c0 + 280 || h.threw.load(); }, 30s);
    INFO("final status g_s=" << (h.status().g_s ? std::to_string(*h.status().g_s) : "none"));
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(moved);

    // Give the gate bridge a few more polls: a lower tag from a NEW provider must not look like a regression.
    evml::test::run_until(mock_ioc, [] { return false; }, 1000ms);
    const auto st = h.status();
    CHECK_FALSE(st.frozen);
    CHECK_FALSE(st.reverify_pending);
    CHECK(st.safe_reorgs == 0);
    CHECK(st.g_s == c0 + 280);
    CHECK(st.g_f == c0 + 230);
    CHECK(h.live());

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

#if defined(EVML_TEST_HOOKS)
TEST_CASE("host: a frozen io thread makes the watchdog abort the process (SIGABRT)", "[integration]") {
    const std::string shard = "t_host_" + random_tag();
    const fs::path dir = fs::temp_directory_path() / ("evml-host-freeze-" + random_tag());
    fs::create_directories(dir);
    Cleanup cleanup{shard, dir};

    // Providers are unreachable on purpose: the freeze must abort the process regardless of the pipeline.
    const std::string dead = "http://127.0.0.1:9/";
    const auto toml = host_toml(shard, dir / "spill", dead, 3000);
    const auto cfg_path = dir / "freeze.toml";
    std::ofstream(cfg_path) << toml;

    // The child resolves provider env vars itself: pass them next to the inherited environment (REDIS_URL).
    std::vector<std::string> env_strings;
    for (char** e = environ; *e; ++e) env_strings.emplace_back(*e);
    for (const char* k : {"BASE_RPC_URL", "BASE_RPC_URL_ALCHEMY", "BASE_RPC_URL_ANKR", "BASE_RPC_URL_QUICKNODE"}) {
        env_strings.push_back(std::string(k) + "=" + dead);
    }
    for (const char* k : {"BASE_WSS_URL", "BASE_WSS_URL_ALCHEMY", "BASE_WSS_URL_QUICKNODE"}) {
        env_strings.push_back(std::string(k) + "=ws://127.0.0.1:9/");
    }
    env_strings.push_back("EVML_HOST_ID=test-host");
    std::vector<char*> envp;
    for (auto& s : env_strings) envp.push_back(s.data());
    envp.push_back(nullptr);

    const std::string exe = EVML_LISTENER_EXE;
    const std::string cfg_arg = cfg_path.string();
    const std::string log_path = (dir / "child.log").string();
    std::vector<char*> argv{const_cast<char*>(exe.c_str()), const_cast<char*>("--config"), const_cast<char*>(cfg_arg.c_str()),
                            const_cast<char*>("--test-freeze-io-after"), const_cast<char*>("1"), nullptr};

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 1, log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    posix_spawn_file_actions_adddup2(&fa, 1, 2);
    pid_t pid = 0;
    const int rc = posix_spawn(&pid, exe.c_str(), &fa, nullptr, argv.data(), envp.data());
    posix_spawn_file_actions_destroy(&fa);
    REQUIRE(rc == 0);

    int status = 0;
    bool exited = false;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (waitpid(pid, &status, WNOHANG) == pid) {
            exited = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    if (!exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
    }
    std::ifstream lf(log_path);
    const std::string child_log((std::istreambuf_iterator<char>(lf)), std::istreambuf_iterator<char>());
    INFO("child log:\n" << child_log);
    REQUIRE(exited);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);  // a shell reports this as exit code 134
    CHECK(child_log.find("io_tick") != std::string::npos);  // the abort line names the stale heartbeat
}
#endif
