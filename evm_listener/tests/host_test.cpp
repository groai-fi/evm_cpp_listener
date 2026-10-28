// Host tests (plan 60 §5.H, slice 5-D1). The in-process tests run the real host (watchdog, ops server, sink, pool,
// sweeper, driver) against the compose Redis (REDIS_URL, process environment only) and an in-process mock JSON-RPC
// server. Each test uses its own random shard id and removes its keys afterwards.
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
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
#include "mock_ws_server.hpp"
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
    std::map<std::uint64_t, std::string> blocks{};  // eth_getBlockByNumber(<hex number>): block JSON; absent -> null
    std::vector<std::uint64_t> block_requests{};     // every numeric eth_getBlockByNumber, in order
    std::vector<std::pair<std::uint64_t, std::string>> logs{};  // eth_getLogs: (block number, log JSON), served by range
};

evml::test::MockHttpServer::Handler tunable_node(NodeState& s) {
    return [&s](const evml::test::MockRequest& r) {
        if (s.fail) return evml::test::MockReply{.status = 500, .body = "node down"};
        const auto v = boost::json::parse(r.body).as_object();
        const std::string method(v.at("method").as_string());
        if (method == "eth_getLogs") ++s.getlogs_calls;
        if (method == "eth_getBlockByNumber") {
            const std::string tag(v.at("params").as_array().at(0).as_string());
            if (tag.rfind("0x", 0) == 0) {  // a block by number (the tags are "safe" / "finalized")
                const auto n = std::stoull(tag, nullptr, 16);
                s.block_requests.push_back(n);
                const auto id = static_cast<std::uint64_t>(v.at("id").as_int64());
                const auto it = s.blocks.find(n);
                return evml::test::MockReply{.body = rpc_reply(id, it == s.blocks.end() ? "null" : it->second)};
            }
        }
        if (method == "eth_getLogs" && !s.logs.empty()) {
            const auto& p = v.at("params").as_array().at(0).as_object();
            const auto from = std::stoull(std::string(p.at("fromBlock").as_string()), nullptr, 16);
            const auto to = std::stoull(std::string(p.at("toBlock").as_string()), nullptr, 16);
            std::string arr = "[";
            for (const auto& [n, json] : s.logs) {
                if (n < from || n > to) continue;
                if (arr.size() > 1) arr += ",";
                arr += json;
            }
            arr += "]";
            return evml::test::MockReply{.body = rpc_reply(static_cast<std::uint64_t>(v.at("id").as_int64()), arr)};
        }
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
    std::atomic<std::uint64_t> ws_missed{0};  // latest on_cross_check totals
    std::atomic<std::uint64_t> ws_extra{0};
    std::atomic<int> cross_checks{0};         // on_cross_check calls (one per swept page)
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
            opt.on_cross_check = [this](std::uint64_t missed, std::uint64_t extra) {
                ws_missed = missed;
                ws_extra = extra;
                ++cross_checks;
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

// ---- 5-D4 helpers: a WS node that pushes newHeads the test queues, and block hashes per branch ----

constexpr const char* kHeadsSub = "0x9ce59a13059e417087c02d3236a0b1cc";
constexpr const char* kLogsSub = "0x4a8a4c0d8a3b4e5f9d7c1b2a3e4f5a6b";

// 32-byte hash of block `n` on branch `branch` ('a' = the canonical chain first fed, 'b' = the fork).
std::string hx(char branch, std::uint64_t n) {
    char num[17];
    std::snprintf(num, sizeof num, "%016llx", static_cast<unsigned long long>(n));
    return std::string("0x") + branch + std::string(47, '0') + num;
}

std::string head_json(std::uint64_t n, const std::string& hash, const std::string& parent) {
    return R"({"number":")" + evml::core::to_hex_u64(n) + R"(","hash":")" + hash + R"(","parentHash":")" + parent +
           R"(","timestamp":")" + evml::core::to_hex_u64(1000 + n) + R"("})";
}

std::string head_msg(std::uint64_t n, const std::string& hash, const std::string& parent) {
    return std::string(R"({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":")") + kHeadsSub +
           R"(","result":)" + head_json(n, hash, parent) + "}}";
}

// A log of config contract 0 (a safe-class pool) at `n` under block hash `hash`; the JSON an eth_getLogs result or a WS
// `logs` frame carries.
std::string cfg_log_json(const evml::core::Config& cfg, std::uint64_t n, const std::string& hash, std::uint64_t idx) {
    const auto& c = cfg.contracts.at(0);
    return R"({"address":")" + c.address.to_hex() + R"(","topics":[")" + c.topics.at(0).to_hex() +
           R"("],"data":"0x","blockNumber":")" + evml::core::to_hex_u64(n) + R"(","blockHash":")" + hash +
           R"(","transactionHash":"0x)" + std::string(64, 'e') + R"(","transactionIndex":"0x0","logIndex":")" +
           evml::core::to_hex_u64(idx) + R"(","removed":false})";
}

std::string logs_msg(const std::string& log_json) {
    return std::string(R"({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":")") + kLogsSub +
           R"(","result":)" + log_json + "}}";
}

struct HeadFeed {
    std::deque<std::string> out;  // frames to send; touched only by the test thread (the mock lives on its ioc)
};

evml::test::MockWsServer::Script head_script(HeadFeed& f) {
    return [&f](evml::test::MockWsConn& c) -> net::awaitable<void> {
        auto r1 = co_await c.read_req();  // eth_chainId
        co_await c.send(rpc_reply(r1.id, "\"0x2105\""));
        auto r2 = co_await c.read_req();  // eth_subscribe newHeads
        co_await c.send(rpc_reply(r2.id, std::string("\"") + kHeadsSub + "\""));
        auto r3 = co_await c.read_req();  // eth_subscribe logs
        co_await c.send(rpc_reply(r3.id, std::string("\"") + kLogsSub + "\""));
        for (;;) {
            if (f.out.empty()) {
                co_await c.sleep(10ms);
                continue;
            }
            const std::string frame = std::move(f.out.front());
            f.out.pop_front();
            co_await c.send(frame);
        }
    };
}

// quicknode (first in ws_order) gets the mock WS node; every other WS URL stays dead.
evml::core::EnvLookup ws_provider_env(const std::string& http_url, const std::string& ws_url) {
    return [http_url, ws_url](const std::string& k) -> std::string {
        if (k.rfind("BASE_RPC_URL", 0) == 0) return http_url;
        if (k == "BASE_WSS_URL_QUICKNODE") return ws_url;
        if (k.rfind("BASE_WSS_URL", 0) == 0) return "ws://127.0.0.1:9/";
        if (k == "EVML_HOST_ID") return "test-host";
        return "";
    };
}

// One host with a live WS node (heads on demand) and an HTTP node whose safe / finalized tags the test sets. The mock
// node sits at c0 + 310 (= safe + 10), so the heads the test feeds around that height agree with the HTTP head.
struct HeadRig {
    std::string shard = "t_host_" + random_tag();
    fs::path spill = fs::temp_directory_path() / ("evml-host-heads-" + random_tag());
    Cleanup cleanup{shard, spill};
    std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns{.safe = c0 + 300, .finalized = c0 + 250};
    HeadFeed feed;
    evml::test::MockHttpServer http{mock_ioc, tunable_node(ns)};
    evml::test::MockWsServer ws{mock_ioc, head_script(feed)};
    RunningHost host{evml::core::Config::parse(host_toml(shard, spill, http.url(), 30000), "heads.toml",
                                               ws_provider_env(http.url(), ws.url()))};

    template <class Pred>
    bool until(Pred p, std::chrono::milliseconds limit) {
        return evml::test::run_until(mock_ioc, p, limit);
    }
    void wait(std::chrono::milliseconds d) { until([] { return false; }, d); }
    bool live() { return until([this] { return host.live() || host.threw.load(); }, 30s) && !host.threw.load(); }
    // Waits until the host stops calling eth_getLogs (all sweeps and re-verifications are done).
    int quiet() {
        int calls = ns.getlogs_calls;
        for (int i = 0; i < 40; ++i) {
            wait(700ms);
            if (ns.getlogs_calls == calls) return calls;
            calls = ns.getlogs_calls;
        }
        FAIL("the host never stopped calling eth_getLogs");
        return calls;
    }
    // Queues heads lo..hi on `branch` (each one's parent is the previous block of the same branch) and waits until the
    // host has read them.
    void feed_chain(char branch, std::uint64_t lo, std::uint64_t hi) {
        for (auto n = lo; n <= hi; ++n) feed.out.push_back(head_msg(n, hx(branch, n), hx(branch, n - 1)));
        REQUIRE(until([this] { return feed.out.empty(); }, 10s));
        wait(500ms);
    }
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

// ---- 5-D4: WS heads -> ReorgDetector -> driver ----

TEST_CASE("host: a head fork below C_s is walked back to the ring and makes the driver re-verify", "[integration]") {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());

    // Canonical chain 297..310 ('a'); the driver sits at C_s = c0 + 300.
    r.feed_chain('a', r.c0 + 297, r.c0 + 310);
    const int before = r.quiet();

    // A fork off block c0 + 298: blocks 299..309 and the head 310 are on branch 'b'.
    for (auto n = r.c0 + 299; n <= r.c0 + 309; ++n) {
        r.ns.blocks[n] = head_json(n, hx('b', n), n == r.c0 + 299 ? hx('a', r.c0 + 298) : hx('b', n - 1));
    }
    r.feed.out.push_back(head_msg(r.c0 + 310, hx('b', r.c0 + 310), hx('b', r.c0 + 309)));
    const bool reverified =
        r.until([&] { return r.ns.getlogs_calls > before || r.host.threw.load(); }, 30s);
    REQUIRE_FALSE(r.host.threw.load());
    REQUIRE(reverified);  // the fork point c0 + 298 is below C_s: eth_getLogs over (C_f, C_s] again

    // The walk asked for exactly the missing ancestors, newest first, and stopped at the ring.
    std::vector<std::uint64_t> expect;
    for (auto n = r.c0 + 309; n >= r.c0 + 299; --n) expect.push_back(n);
    CHECK(r.ns.block_requests == expect);

    REQUIRE(r.until([&] { return !r.host.status().reverify_pending && r.host.live(); }, 15s));
    CHECK_FALSE(r.host.status().frozen);
    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
}

TEST_CASE("host: a head fork above C_s is walked but does not trigger a re-verification", "[integration]") {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());

    r.feed_chain('a', r.c0 + 297, r.c0 + 310);
    const int before = r.quiet();

    // Fork off block c0 + 308 (>= C_s = c0 + 300): only block 309 and the head 310 differ.
    r.ns.blocks[r.c0 + 309] = head_json(r.c0 + 309, hx('b', r.c0 + 309), hx('a', r.c0 + 308));
    r.feed.out.push_back(head_msg(r.c0 + 310, hx('b', r.c0 + 310), hx('b', r.c0 + 309)));
    REQUIRE(r.until([&] { return !r.ns.block_requests.empty() || r.host.threw.load(); }, 15s));
    REQUIRE_FALSE(r.host.threw.load());
    r.wait(1500ms);  // time for a (wrongly) triggered re-verification to show up

    CHECK(r.ns.block_requests == std::vector<std::uint64_t>{r.c0 + 309});
    CHECK(r.ns.getlogs_calls == before);
    CHECK_FALSE(r.host.status().reverify_pending);
    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
}

TEST_CASE("host: a head gap too long to walk resyncs the ring and makes the driver re-verify", "[integration]") {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());

    r.feed_chain('a', r.c0 + 297, r.c0 + 310);
    // The chain moves on 17 blocks (a WS outage): raise the HTTP node with it, so the pool sees no lag.
    r.ns.safe = r.c0 + 317;
    r.ns.finalized = r.c0 + 267;
    REQUIRE(r.until([&] { return r.host.status().g_s == r.c0 + 317 || r.host.threw.load(); }, 15s));
    const int before = r.quiet();  // the sweeps caught up with the new gates

    r.feed.out.push_back(head_msg(r.c0 + 327, hx('a', r.c0 + 327), hx('a', r.c0 + 326)));  // 17 > the walk limit of 16
    const bool reverified =
        r.until([&] { return r.ns.getlogs_calls > before || r.host.threw.load(); }, 15s);
    REQUIRE_FALSE(r.host.threw.load());
    REQUIRE(reverified);
    CHECK(r.ns.block_requests.empty());  // no walk was started: a 17-block walk would hold the provider's bucket

    REQUIRE(r.until([&] { return !r.host.status().reverify_pending && r.host.live(); }, 15s));
    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
}

namespace {
struct CrossCheckTotals {
    std::uint64_t missed;
    std::uint64_t extra;
    int pages;
};

// Live host at c0 + 300; then one log at block c0 + 320 is (optionally) delivered over WS and (optionally) served by
// eth_getLogs, and the gates move past it so the safe and the final sweep both cover it. Returns the cross-check totals.
CrossCheckTotals run_cross_check(bool via_ws, bool via_sweep) {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());
    r.quiet();

    const std::uint64_t n = r.c0 + 320;
    const std::string hash = hx('a', n);
    const std::string log = cfg_log_json(r.host.cfg, n, hash, 0);
    r.ns.blocks[n] = head_json(n, hash, hx('a', n - 1));  // the canonical block the sweep validates the log against
    if (via_sweep) r.ns.logs.emplace_back(n, log);
    if (via_ws) {
        r.feed.out.push_back(logs_msg(log));
        REQUIRE(r.until([&] { return r.feed.out.empty(); }, 10s));
        r.wait(500ms);
    }

    const int before = r.host.cross_checks.load();
    r.ns.safe = r.c0 + 340;
    r.ns.finalized = r.c0 + 330;
    REQUIRE(r.until([&] { return r.host.status().c_f >= r.c0 + 330 || r.host.threw.load(); }, 30s));
    REQUIRE_FALSE(r.host.threw.load());
    r.quiet();
    const CrossCheckTotals out{r.host.ws_missed.load(), r.host.ws_extra.load(), r.host.cross_checks.load() - before};
    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
    return out;
}
}  // namespace

TEST_CASE("host: a WS log that the sweep also returns is a clean cross-check", "[integration]") {
    const auto t = run_cross_check(true, true);
    CHECK(t.pages >= 2);  // the safe page and the final page
    CHECK(t.missed == 0);
    CHECK(t.extra == 0);
}

TEST_CASE("host: a log only the sweep returns is counted as a WS miss", "[integration]") {
    const auto t = run_cross_check(false, true);
    CHECK(t.pages >= 2);
    CHECK(t.missed == 1);
    CHECK(t.extra == 0);
}

TEST_CASE("host: a log only WS delivered is counted as WS extra", "[integration]") {
    const auto t = run_cross_check(true, false);
    CHECK(t.pages >= 2);
    CHECK(t.missed == 0);
    CHECK(t.extra == 1);
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
