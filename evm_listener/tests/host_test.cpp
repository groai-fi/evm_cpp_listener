// Host tests (plan 60 §5.H, slice 5-D1). The in-process tests run the real host (watchdog, ops server, sink, pool,
// sweeper, driver) against the compose Redis (REDIS_URL, process environment only) and an in-process mock JSON-RPC
// server. Each test uses its own random shard id and removes its keys afterwards.
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
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
    replace_once(t, "heartbeat_s             = 10", "heartbeat_s             = 1");
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
    std::atomic<std::uint16_t> ops_port{0};
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
            opt.on_ops_port = [this](std::uint16_t p) { ops_port = p; };
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

struct OpsReply {
    int status{};
    std::string body;
};

OpsReply ops_get(std::uint16_t port, std::string_view target) {
    REQUIRE(port != 0);
    net::io_context ioc;
    net::ip::tcp::socket sock(ioc);
    sock.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
    boost::beast::http::request<boost::beast::http::empty_body> req{boost::beast::http::verb::get, target, 11};
    req.set(boost::beast::http::field::host, "127.0.0.1");
    req.set(boost::beast::http::field::connection, "close");
    boost::beast::http::write(sock, req);
    boost::beast::flat_buffer buf;
    boost::beast::http::response<boost::beast::http::string_body> res;
    boost::beast::http::read(sock, buf, res);
    return OpsReply{static_cast<int>(res.result_int()), res.body()};
}

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

    const auto m = ops_get(h.ops_port.load(), "/metrics");
    CHECK(m.status == 200);
    CHECK_THAT(m.body, Catch::Matchers::ContainsSubstring("evml_role{role=\"standby\"} 1"));
    CHECK_THAT(m.body, Catch::Matchers::ContainsSubstring("evml_role{role=\"leader\"} 0"));
    CHECK_THAT(m.body, Catch::Matchers::ContainsSubstring("evml_checkpoint_block{class=\"safe\"} 0"));

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
    // Let the dead WS providers (quicknode/infura/alchemy -> 127.0.0.1:9) finish failing over so `ankr` settles as
    // `current_getlogs()` and records `finalized = c0 + 250` before we regress the tag.
    evml::test::run_until(mock_ioc, [] { return false; }, 1000ms);
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

// Sets a process environment variable for one test and removes it afterwards.
struct ScopedEnv {
    std::vector<std::string> names;
    void set(const std::string& k, const std::string& v) {
        ::setenv(k.c_str(), v.c_str(), 1);
        names.push_back(k);
    }
    ~ScopedEnv() {
        for (const auto& k : names) ::unsetenv(k.c_str());
    }
};

TEST_CASE("host: a healthy leader pings the dead-man URL and a frozen release posts an alert to the webhook",
          "[integration]") {
    evml::core::log::init("info");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-alerts-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns{.safe = c0 + 300, .finalized = c0 + 250};
    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
    std::vector<evml::test::MockRequest> pings, posts;
    evml::test::MockHttpServer deadman(mock_ioc, [&](const evml::test::MockRequest& r) {
        pings.push_back(r);
        return evml::test::MockReply{.body = "OK"};
    });
    evml::test::MockHttpServer webhook(mock_ioc, [&](const evml::test::MockRequest& r) {
        posts.push_back(r);
        return evml::test::MockReply{.body = "{}"};
    });

    auto toml = host_toml(shard, spill, node.url(), 30000);
    replace_once(toml, "deadman_ping_s          = 60", "deadman_ping_s          = 1");
    auto cfg = evml::core::Config::parse(toml, "alerts.toml", provider_env(node.url()));
    ScopedEnv env;  // declared before the host so it is removed after the host thread has ended
    env.set(cfg.ops.deadman_ping_env, deadman.url() + "ping/secret-uuid");
    env.set(cfg.ops.alert_webhook_env, webhook.url() + "hook");
    env.set(cfg.ops.alert_webhook_token_env, "host-test-token");
    RunningHost h(std::move(cfg));
    h.start();

    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return !pings.empty(); }, 10s));
    CHECK(pings.front().method == "GET");
    CHECK(pings.front().target == "/ping/secret-uuid");
    CHECK(posts.empty());  // before the first 10 s evaluation, nothing has been posted yet

    // Let the dead WS providers (quicknode/infura/alchemy -> 127.0.0.1:9) finish failing over so `ankr` settles as
    // `current_getlogs()` and records `finalized = c0 + 250` before we regress the tag.
    evml::test::run_until(mock_ioc, [] { return false; }, 1000ms);

    ns.finalized = c0 + 100;  // an impossible regression of the finalized tag: release frozen
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.status().frozen || h.threw.load(); }, 15s));
    REQUIRE_FALSE(h.threw.load());

    const auto find_gate_regressed = [&]() -> const evml::test::MockRequest* {
        for (const auto& req : posts) {
            const auto obj = boost::json::parse(req.body).as_object();
            if (std::string(obj.at("title").as_string()).find("Gate Regressed") != std::string::npos) {
                return &req;
            }
        }
        return nullptr;
    };
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return find_gate_regressed() != nullptr || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());
    const auto* gate_req = find_gate_regressed();
    REQUIRE(gate_req != nullptr);
    const auto body = boost::json::parse(gate_req->body).as_object();
    CHECK(body.at("severity").as_string() == "critical");
    CHECK(std::string(body.at("title").as_string()).find("Gate Regressed") != std::string::npos);
    CHECK(body.at("shard").as_string() == shard);
    CHECK(body.at("host").as_string() == "test-host");
    bool has_key = false;
    for (const auto& [k, v] : gate_req->headers) has_key |= (k == "X-API-Key" && v == "host-test-token");
    CHECK(has_key);

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

TEST_CASE("host: the leader publishes the plan 4.7 heartbeat payload under hb:<host_id> with a TTL", "[integration]") {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());
    r.feed_chain('a', r.c0 + 297, r.c0 + 310);

    sw::redis::Redis admin{require_env("REDIS_URL")};
    const evml::sink::Keys keys{r.shard};
    const std::string key = keys.hb("test-host");
    boost::json::object hb;
    // Wait for a payload that already knows the ring head (the first one may predate the heads).
    const bool seen = r.until(
        [&] {
            const auto v = admin.get(key);
            if (!v) return false;
            hb = boost::json::parse(*v).as_object();
            return hb.at("head").is_uint64() || hb.at("head").is_int64();
        },
        15s);
    REQUIRE(seen);
    CHECK(hb.at("role").as_string() == "leader");
    CHECK(hb.at("state").as_string() == "live");
    CHECK(hb.at("head").to_number<std::uint64_t>() == r.c0 + 310);
    CHECK(hb.at("G_s").to_number<std::uint64_t>() == r.c0 + 300);
    CHECK(hb.at("G_f").to_number<std::uint64_t>() == r.c0 + 250);
    CHECK(hb.at("C_s").to_number<std::uint64_t>() == r.c0 + 300);
    CHECK(hb.at("C_f").to_number<std::uint64_t>() == r.c0 + 250);
    CHECK_FALSE(hb.at("active_provider").as_string().empty());
    CHECK_FALSE(hb.at("version").as_string().empty());
    CHECK(hb.at("epoch_n").is_null());  // lease disabled: there is no epoch
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
    CHECK(static_cast<std::int64_t>(now_ms) - hb.at("ts").to_number<std::int64_t>() < 10000);
    const auto ttl = admin.ttl(key);
    CHECK(ttl > 0);
    CHECK(ttl <= 30);
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

TEST_CASE("host: historical logs swept during CatchingUp do not count as WS misses", "[integration]") {
    evml::core::log::init("info");
    HeadRig r;
    // Place historical logs in [c0 + 1, c0 + 250] and in the pre-live released window (c0 + 250, c0 + 300].
    const std::uint64_t n1 = r.c0 + 200;
    const std::string h1 = hx('a', n1);
    r.ns.blocks[n1] = head_json(n1, h1, hx('a', n1 - 1));
    r.ns.logs.emplace_back(n1, cfg_log_json(r.host.cfg, n1, h1, 0));

    const std::uint64_t n2 = r.c0 + 280;
    const std::string h2 = hx('a', n2);
    r.ns.blocks[n2] = head_json(n2, h2, hx('a', n2 - 1));
    r.ns.logs.emplace_back(n2, cfg_log_json(r.host.cfg, n2, h2, 0));

    r.host.start();
    REQUIRE(r.live());
    r.quiet();
    CHECK(r.host.ws_missed.load() == 0);
    CHECK(r.host.ws_extra.load() == 0);

    // Advance finalized across the pre-live / post-live boundary [c0 + 251, c0 + 330]; n2 at c0 + 280 is pre-live.
    r.ns.safe = r.c0 + 340;
    r.ns.finalized = r.c0 + 330;
    REQUIRE(r.until([&] { return r.host.status().c_f >= r.c0 + 330 || r.host.threw.load(); }, 30s));
    REQUIRE_FALSE(r.host.threw.load());
    r.quiet();
    CHECK(r.host.ws_missed.load() == 0);
    CHECK(r.host.ws_extra.load() == 0);

    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
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

TEST_CASE("host: taking the lease over from another host fires Leader Changed once and the heartbeat carries epoch_n", "[integration]") {
    evml::core::log::init("info");
    const std::string redis_url = require_env("REDIS_URL");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-leader-" + random_tag());
    Cleanup cleanup{shard, spill};

    // The previous term (epoch 5) belonged to hostOld and its lease key has expired: only the epoch key remains.
    sw::redis::Redis admin{redis_url};
    const evml::sink::Keys keys{shard};
    admin.set(keys.epoch(), "5|hostOld|oldnonce");

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    std::vector<evml::test::MockRequest> posts;
    evml::test::MockHttpServer webhook(mock_ioc, [&](const evml::test::MockRequest& r) {
        posts.push_back(r);
        return evml::test::MockReply{.body = "{}"};
    });

    auto cfg = evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000, true), "leader.toml",
                                         provider_env(node.url(), "hostA"));
    ScopedEnv env;  // declared before the host so it is removed after the host thread has ended
    env.set(cfg.ops.alert_webhook_env, webhook.url() + "hook");
    env.set(cfg.ops.alert_webhook_token_env, "host-test-token");
    RunningHost h(std::move(cfg));
    h.start();

    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return !posts.empty() || h.threw.load(); }, 30s));  // first evaluation
    REQUIRE_FALSE(h.threw.load());

    const auto body = boost::json::parse(posts.front().body).as_object();
    CHECK(body.at("title").as_string() == "Leader Changed");
    CHECK(body.at("severity").as_string() == "warning");
    CHECK(body.at("details").as_object().at("old_host").as_string() == "hostOld");
    CHECK(body.at("details").as_object().at("new_host").as_string() == "hostA");

    // This term is epoch 6: the heartbeat says so.
    boost::json::object hb;
    REQUIRE(evml::test::run_until(
        mock_ioc,
        [&] {
            const auto v = admin.get(keys.hb("hostA"));
            if (!v) return false;
            hb = boost::json::parse(*v).as_object();
            return true;
        },
        15s));
    CHECK(hb.at("epoch_n").to_number<std::uint64_t>() == 6);

    // Once per occurrence: nothing more is posted for it.
    evml::test::run_until(mock_ioc, [] { return false; }, 12s);
    int leader_changed = 0;
    for (const auto& p : posts) {
        if (boost::json::parse(p.body).as_object().at("title").as_string() == "Leader Changed") ++leader_changed;
    }
    CHECK(leader_changed == 1);

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

// infura (2nd in ws_order) gets the mock WS node; quicknode (1st) and alchemy stay dead.
evml::core::EnvLookup infura_ws_env(const std::string& http_url, const std::string& ws_url) {
    return [http_url, ws_url](const std::string& k) -> std::string {
        if (k.rfind("BASE_RPC_URL", 0) == 0) return http_url;
        if (k == "BASE_WSS_URL") return ws_url;
        if (k.rfind("BASE_WSS_URL", 0) == 0) return "ws://127.0.0.1:9/";
        if (k == "EVML_HOST_ID") return "test-host";
        return "";
    };
}

// Slow (about 70 s): the pool allows a voluntary switch only 60 s after the last one (Policy::min_voluntary_gap, not
// configurable), and a Down provider is re-probed every 30 s (Policy::probe_interval).
TEST_CASE("host: failing over and then switching back to the primary raises Provider Failover for both, told apart by reason",
          "[integration]") {
    evml::core::log::init("info");
    const std::string shard = "t_host_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-switchback-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    HeadFeed feed;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    evml::test::MockWsServer ws(mock_ioc, head_script(feed));
    std::vector<evml::test::MockRequest> posts;
    evml::test::MockHttpServer webhook(mock_ioc, [&](const evml::test::MockRequest& r) {
        posts.push_back(r);
        return evml::test::MockReply{.body = "{}"};
    });

    // quicknode's WS is dead: three session failures take it Down, the pool fails over to infura (mock WS). Its HTTP
    // answers the probe, so it comes back Up, and after prefer_primary_after the pool switches back to it. Heads keep
    // flowing from infura (a base chain first, then one per second): a silent WS would be failed as stalled and the pool
    // would cascade through every provider instead of settling on infura.
    for (auto n = c0 + 297; n <= c0 + 310; ++n) feed.out.push_back(head_msg(n, hx('a', n), hx('a', n - 1)));
    auto toml = host_toml(shard, spill, node.url(), 30000);
    replace_once(toml, "prefer_primary_after_s = 600", "prefer_primary_after_s = 1");
    auto cfg = evml::core::Config::parse(toml, "switchback.toml", infura_ws_env(node.url(), ws.url()));
    ScopedEnv env;  // declared before the host so it is removed after the host thread has ended
    env.set(cfg.ops.alert_webhook_env, webhook.url() + "hook");
    env.set(cfg.ops.alert_webhook_token_env, "host-test-token");
    RunningHost h(std::move(cfg));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    const auto find_reason = [&](const std::string& reason) -> std::optional<boost::json::object> {
        for (const auto& p : posts) {
            const auto body = boost::json::parse(p.body).as_object();
            if (body.at("title").as_string() != "Provider Failover") continue;
            const auto& d = body.at("details").as_object();
            if (d.at("reason").as_string() == reason) return body;
        }
        return std::nullopt;
    };

    std::uint64_t next_head = c0 + 311;
    auto last_head = std::chrono::steady_clock::now();
    const auto keep_heads_flowing = [&] {
        const auto now = std::chrono::steady_clock::now();
        if (now - last_head < 1s) return;
        feed.out.push_back(head_msg(next_head, hx('a', next_head), hx('a', next_head - 1)));
        ++next_head;
        last_head = now;
    };
    REQUIRE(evml::test::run_until(
        mock_ioc,
        [&] {
            keep_heads_flowing();
            return find_reason("switch_back").has_value() || h.threw.load();
        },
        120s));
    REQUIRE_FALSE(h.threw.load());

    // One occurrence, one POST: the failover was evaluated about six times (every 10 s) before the switch back, and
    // was sent exactly once before the switch-back event arrived.
    int failover_posts_before_switch_back = 0;
    for (const auto& p : posts) {
        const auto body = boost::json::parse(p.body).as_object();
        if (body.at("title").as_string() != "Provider Failover") continue;
        const auto reason = body.at("details").as_object().at("reason").as_string();
        if (reason == "switch_back") break;
        if (reason == "failover") ++failover_posts_before_switch_back;
    }
    CHECK(failover_posts_before_switch_back == 1);

    const auto failover = find_reason("failover");
    REQUIRE(failover);
    CHECK(failover->at("details").as_object().at("from").as_string() == "quicknode");
    CHECK(failover->at("details").as_object().at("to").as_string() == "infura");
    CHECK(failover->at("message").as_string() == "the WS provider failed over from quicknode to infura");

    const auto back = find_reason("switch_back");
    REQUIRE(back);
    CHECK(back->at("details").as_object().at("from").as_string() == "infura");
    CHECK(back->at("details").as_object().at("to").as_string() == "quicknode");
    CHECK(back->at("message").as_string() == "the WS provider switched back from infura to quicknode");
    CHECK(back->at("severity").as_string() == "warning");

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

TEST_CASE("host: /metrics exposes every §5.4 and sink/sweeper metric and updates on scrape", "[integration][host]") {
    using Catch::Matchers::ContainsSubstring;
    evml::core::log::init("info");
    const std::string shard = "host_metrics_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-metrics-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    HeadFeed feed;
    evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
    evml::test::MockWsServer ws(mock_ioc, head_script(feed));
    auto cfg = evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000), "metrics.toml",
                                         ws_provider_env(node.url(), ws.url()));
    feed.out.push_back(head_msg(c0 + 300, hx('a', c0 + 300), hx('a', c0 + 299)));
    feed.out.push_back(logs_msg(cfg_log_json(cfg, c0 + 300, hx('a', c0 + 300), 0)));

    RunningHost h(std::move(cfg));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return (h.live() && feed.out.empty()) || h.threw.load(); }, 30s));
    evml::test::run_until(mock_ioc, [] { return false; }, 100ms);
    REQUIRE_FALSE(h.threw.load());

    const auto m = ops_get(h.ops_port.load(), "/metrics");
    CHECK(m.status == 200);

    // Every §5.4 metric plus existing sink, sweeper and pipeline metrics:
    const std::vector<std::string> expected_names{
        "evml_role",
        "evml_lease_renew_failures_total",
        "evml_provider_active",
        "evml_provider_failovers_total",
        "evml_alerts_fired_total",
        "evml_provider_inconsistency_total",
        "evml_rpc_requests_total",
        "evml_ws_messages_total",
        "evml_safe_missed_total",
        "evml_reverify_total",
        "evml_getlogs_throttled_total",
        "evml_consumer_undelivered_age_seconds",
        "evml_consumer_pending_age_seconds",
        "evml_consumer_lag_entries",
        "evml_spill_discarded_records_total",
        "evml_alert_send_failures_total",
        "evml_deadman_ping_failures_total",
        "evml_ws_missed_logs_total",
        "evml_ws_extra_logs_total",
        "evml_pending_evicted_logs_total",
        "evml_sink_dup_total",
        "evml_sink_below_finalized_total",
        "evml_sink_heartbeat_failures_total",
        "evml_sink_consumer_lag_failures_total",
        "evml_sink_reconnects_total",
        "evml_sink_queue_depth",
        "evml_sink_spill_records",
        "evml_sink_spill_bytes",
        "evml_sink_pending_writes",
        "evml_sink_oldest_record_age_seconds",
        "evml_sweep_emitted_safe_total",
        "evml_sweep_emitted_final_total",
        "evml_sweep_deduped_total",
        "evml_reverts_emitted_total",
        "evml_sweep_matched_total",
        "evml_sweep_range_shrinks_total",
        "evml_sweep_failure_shrinks_total",
        "evml_sweep_volume_shrinks_total",
        "evml_sweep_reverified_new_total",
        "evml_sweep_address_splits_total",
        "evml_head_block",
        "evml_gate_block",
        "evml_checkpoint_block",
    };
    for (const auto& name : expected_names) {
        INFO("metric: " << name);
        CHECK_THAT(m.body, ContainsSubstring("# TYPE " + name + " "));
    }
    CHECK_THAT(m.body, ContainsSubstring("evml_role{role=\"leader\"} 1"));
    CHECK_THAT(m.body, ContainsSubstring("evml_role{role=\"standby\"} 0"));
    CHECK_THAT(m.body, ContainsSubstring("evml_checkpoint_block{class=\"safe\"} " + std::to_string(c0 + 300)));
    CHECK_THAT(m.body, ContainsSubstring("evml_checkpoint_block{class=\"finalized\"} " + std::to_string(c0 + 250)));
    CHECK_THAT(m.body, ContainsSubstring("evml_ws_messages_total{provider=\"quicknode\",stream=\"heads\"} 1"));
    CHECK_THAT(m.body, ContainsSubstring("evml_ws_messages_total{provider=\"quicknode\",stream=\"logs\"} 1"));

    h.finish();
    CHECK(h.exit_code.load() == 0);
}

TEST_CASE("host: /readyz returns 200 for leader-live, standby and lease-disabled, and 503 when saturated or gate is stuck",
          "[integration][host]") {
    evml::core::log::init("info");
    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());

    // 1. Leader-live with lease-disabled, plus D6 gate-age check (safe_gate_stuck_s = 3).
    {
        const std::string shard = "host_ready_live_" + random_tag();
        const fs::path spill = fs::temp_directory_path() / ("evml-host-ready-live-" + random_tag());
        Cleanup cleanup{shard, spill};

        net::io_context mock_ioc;
        NodeState ns;
        ns.safe = c0 + 300;
        ns.finalized = c0 + 250;
        evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
        auto toml = host_toml(shard, spill, node.url(), 30000);
        replace_once(toml, "safe_gate_stuck_s    = 600", "safe_gate_stuck_s    = 3");
        auto cfg = evml::core::Config::parse(toml, "ready_live.toml", provider_env(node.url()));

        RunningHost h(std::move(cfg));
        h.start();
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
        REQUIRE_FALSE(h.threw.load());

        const auto hz = ops_get(h.ops_port.load(), "/healthz");
        CHECK(hz.status == 200);
        CHECK(hz.body == "ok\n");

        const auto rz = ops_get(h.ops_port.load(), "/readyz");
        CHECK(rz.status == 200);
        const auto body = boost::json::parse(rz.body).as_object();
        CHECK(body.at("state").as_string() == "live");
        CHECK(body.at("role").as_string() == "leader");
        CHECK(body.at("lease").as_string() == "disabled");
        CHECK(body.at("deadman").as_string() == "disabled");
        CHECK(body.at("consumer_lag").as_string() == "enabled");
        CHECK_FALSE(body.at("saturated").as_bool());
        CHECK(body.at("gates").as_object().at("safe").to_number<std::uint64_t>() == c0 + 300);
        CHECK(body.at("gates").as_object().at("finalized").to_number<std::uint64_t>() == c0 + 250);
        CHECK(body.at("cursors").as_object().at("safe").to_number<std::uint64_t>() == c0 + 300);
        CHECK(body.at("cursors").as_object().at("finalized").to_number<std::uint64_t>() == c0 + 250);

        // Let 3.2 s pass without G_s advancing -> D6 gate age expires -> 503.
        const auto until = std::chrono::steady_clock::now() + 3200ms;
        evml::test::run_until(mock_ioc, [&] { return std::chrono::steady_clock::now() >= until; }, 4s);
        CHECK(ops_get(h.ops_port.load(), "/readyz").status == 503);

        // Advance G_s -> gate age resets -> 200 again.
        ns.safe = c0 + 305;
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return ops_get(h.ops_port.load(), "/readyz").status == 200; }, 5s));

        h.finish();
        CHECK(h.exit_code.load() == 0);
    }

    // 2. Healthy standby (lease enabled, another host holds the lease in Redis) -> 200.
    {
        const std::string shard = "host_ready_standby_" + random_tag();
        const fs::path spill = fs::temp_directory_path() / ("evml-host-ready-standby-" + random_tag());
        Cleanup cleanup{shard, spill};

        sw::redis::Redis admin{require_env("REDIS_URL")};
        const evml::sink::Keys keys{shard};
        admin.set(keys.leader(), "1|other-host|nonce1", 30s);
        admin.set(keys.epoch(), "1|other-host|nonce1");

        net::io_context mock_ioc;
        evml::test::MockHttpServer node(mock_ioc, healthy_node(c0 + 300, c0 + 250));
        auto cfg = evml::core::Config::parse(host_toml(shard, spill, node.url(), 30000, true, 30000, 500), "ready_standby.toml",
                                             provider_env(node.url(), "standby-host"));
        RunningHost h(std::move(cfg));
        h.start();
        REQUIRE(evml::test::run_until(
            mock_ioc,
            [&] {
                if (h.ops_port.load() == 0) return false;
                const auto r = ops_get(h.ops_port.load(), "/readyz");
                return r.status == 200;
            },
            15s));
        const auto rz = ops_get(h.ops_port.load(), "/readyz");
        CHECK(rz.status == 200);
        const auto body = boost::json::parse(rz.body).as_object();
        CHECK(body.at("state").as_string() == "standby");
        CHECK(body.at("role").as_string() == "standby");
        CHECK(body.at("lease").as_string() == "enabled");
        CHECK(body.at("gates").as_object().at("safe").is_null());
        CHECK(body.at("cursors").as_object().at("safe").is_null());

        h.finish();
        CHECK(h.exit_code.load() == 0);
    }

    // 3. Leader-saturated -> 503.
    {
        const std::string shard = "host_ready_sat_" + random_tag();
        const fs::path spill = fs::temp_directory_path() / ("evml-host-ready-sat-" + random_tag());
        Cleanup cleanup{shard, spill};

        evml::testing::ToxiRedis px;
        ScopedEnv env;
        env.set("EVML_TEST_TOXI_REDIS_URL", px.url());

        net::io_context mock_ioc;
        NodeState ns;
        ns.safe = c0 + 300;
        ns.finalized = c0 + 250;
        evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
        auto toml = host_toml(shard, spill, node.url(), 30000);
        replace_once(toml, "redis_url_env      = \"REDIS_URL\"", "redis_url_env      = \"EVML_TEST_TOXI_REDIS_URL\"");
        replace_once(toml, "queue_capacity     = 100000", "queue_capacity     = 2");
        replace_once(toml, "spill_max_bytes    = 536870912", "spill_max_bytes    = 1");
        auto cfg = evml::core::Config::parse(toml, "ready_sat.toml", provider_env(node.url()));

        RunningHost h(std::move(cfg));
        h.start();
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
        REQUIRE_FALSE(h.threw.load());
        CHECK(ops_get(h.ops_port.load(), "/readyz").status == 200);

        // Disable Redis and advance the safe gate across multiple steps so checkpoint writes overflow the 2-item queue
        // into the 1-byte spill and saturate the sink.
        px.disable();
        for (int i = 1; i <= 5; ++i) {
            ns.safe = c0 + 300 + i * 10;
            const auto step_end = std::chrono::steady_clock::now() + 300ms;
            evml::test::run_until(mock_ioc, [&] { return std::chrono::steady_clock::now() >= step_end; }, 1s);
        }
        REQUIRE(evml::test::run_until(
            mock_ioc,
            [&] {
                const auto r = ops_get(h.ops_port.load(), "/readyz");
                const auto b = boost::json::parse(r.body).as_object();
                return r.status == 503 && b.at("saturated").as_bool();
            },
            15s));

        px.enable();
        h.finish();
        CHECK(h.exit_code.load() == 0);
    }
}

// ---- 5-G: operator tools (--check-redis and --backfill-contract with D8 lease fencing) ----

TEST_CASE("tools: --check-redis prints address_family and startup check results, and exits 1 when unreachable",
          "[integration][tools]") {
    using Catch::Matchers::ContainsSubstring;
    const auto cfg = evml::test::load_base_config();

    std::ostringstream captured;
    auto* old_cout = std::cout.rdbuf(captured.rdbuf());
    const int rc_ok = evml::app::check_redis(cfg);
    std::cout.rdbuf(old_cout);

    CHECK(rc_ok == evml::app::kExitOk);
    CHECK_THAT(captured.str(), ContainsSubstring("address_family=ipv"));
    CHECK_THAT(captured.str(), ContainsSubstring("maxmemory_policy=noeviction"));

    // Unreachable Redis fails fast on the first attempt and exits 1.
    ScopedEnv env;
    env.set("EVML_TEST_DEAD_REDIS_URL", "redis://127.0.0.1:9");
    auto bad_cfg = cfg;
    bad_cfg.sink.redis_url_env = "EVML_TEST_DEAD_REDIS_URL";
    CHECK(evml::app::check_redis(bad_cfg) == evml::app::kExitFailure);
}

TEST_CASE("tools: --backfill-contract emits finalized below C_f and safe/released in C_f..C_s, updates manifest, is idempotent, and enforces D8 lease fencing",
          "[integration][tools]") {
    evml::core::log::init("info");
    const std::string shard = "host_backfill_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-backfill-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns;
    ns.safe = c0 + 300;
    ns.finalized = c0 + 250;
    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));

    // 1. Cold-start the listener with the base contracts so Redis has ckpt {safe=c0+300, finalized=c0+250, contracts}.
    const std::string base_toml = host_toml(shard, spill, node.url(), 30000, true, 10000, 500);
    {
        auto cfg1 = evml::core::Config::parse(base_toml, "bf_init.toml", provider_env(node.url(), "hostA"));
        RunningHost h(std::move(cfg1));
        h.start();
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
        REQUIRE_FALSE(h.threw.load());
        h.finish();
        CHECK(h.exit_code.load() == 0);
    }

    // 2. Add a new safe-class contract with start_block = c0 + 100 (<= C_s = c0 + 300).
    const std::string extra_addr = "0x1111111111111111111111111111111111111111";
    std::string expanded_toml = base_toml + R"toml(
[[contracts]]
name        = "backfill_pool"
address     = ")toml" + extra_addr + R"toml("
finality    = "safe"
stream      = "events"
start_block = )toml" + std::to_string(c0 + 100) + R"toml(
events = [
  "Swap(address,address,int256,int256,uint160,uint128,int24)",
]
)toml";
    auto cfg2 = evml::core::Config::parse(expanded_toml, "bf_expanded.toml", provider_env(node.url(), "hostA"));
    const evml::core::ContractCfg* bf_contract = nullptr;
    for (const auto& c : cfg2.contracts) {
        if (c.name == "backfill_pool") bf_contract = &c;
    }
    REQUIRE(bf_contract != nullptr);

    // Starting the host before backfilling exits 2 (uncovered contract).
    {
        RunningHost h(cfg2);
        h.start();
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.exit_code.load() != -1 || h.threw.load(); }, 15s));
        h.finish();
        CHECK(h.exit_code.load() == evml::app::kExitUsage);
    }

    // Seed two logs for `backfill_pool`: one at c0 + 200 (<= C_f) and one at c0 + 280 (in (C_f, C_s]).
    const auto mk_bf_log = [&](std::uint64_t n) {
        const std::string bh = hx('a', n);
        ns.blocks[n] = head_json(n, bh, hx('a', n - 1));
        return R"({"address":")" + bf_contract->address.to_hex() + R"(","topics":[")" +
               bf_contract->topics.at(0).to_hex() + R"("],"data":"0x","blockNumber":")" + evml::core::to_hex_u64(n) +
               R"(","blockHash":")" + bh + R"(","transactionHash":"0x)" + std::string(64, 'd') +
               R"(","transactionIndex":"0x0","logIndex":"0x0","removed":false})";
    };
    ns.logs = {{c0 + 200, mk_bf_log(c0 + 200)}, {c0 + 280, mk_bf_log(c0 + 280)}};

    sw::redis::Redis admin{require_env("REDIS_URL")};
    const evml::sink::Keys keys{shard};

    // 3. D8: while another host holds the lease, --backfill-contract exits 1 within timeout and writes nothing.
    admin.set(keys.leader(), "9|other-host|nonce9", 30s);
    admin.set(keys.epoch(), "9|other-host|nonce9");
    {
        std::atomic<int> bf_rc{-1};
        std::thread t([&] { bf_rc = evml::app::backfill_contract(cfg2, "backfill_pool", 600ms); });
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return bf_rc.load() != -1; }, 5s));
        t.join();
        CHECK(bf_rc.load() == evml::app::kExitFailure);
        CHECK(admin.xlen(keys.stream(evml::core::StreamKind::Events)) == 0);
    }
    admin.del(keys.leader());  // free the lease

    // 4. With the lease free, --backfill-contract acquires the lease, emits [start_block, C_f] as finalized and
    // (C_f, C_s] as safe (tracked in released), updates ckpt.contracts, and releases the lease on completion.
    {
        std::atomic<int> bf_rc{-1};
        std::thread t([&] { bf_rc = evml::app::backfill_contract(cfg2, "backfill_pool", 10s); });
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return bf_rc.load() != -1; }, 30s));
        t.join();
        REQUIRE(bf_rc.load() == evml::app::kExitOk);
    }
    CHECK(admin.exists(keys.leader()) == 0);  // lease released at the end
    CHECK(admin.exists(keys.epoch()) == 1);

    using StreamEntry = std::pair<std::string, std::unordered_map<std::string, std::string>>;
    std::vector<StreamEntry> entries;
    admin.xrange(keys.stream(evml::core::StreamKind::Events), "-", "+", std::back_inserter(entries));
    REQUIRE(entries.size() == 2);
    CHECK(entries[0].second.at("contract") == "backfill_pool");
    CHECK(std::stoull(entries[0].second.at("block_number")) == c0 + 200);
    CHECK(entries[0].second.at("finality") == "finalized");
    CHECK(entries[1].second.at("contract") == "backfill_pool");
    CHECK(std::stoull(entries[1].second.at("block_number")) == c0 + 280);
    CHECK(entries[1].second.at("finality") == "safe");

    // Only the safe-window event (c0 + 280) is tracked in `released`.
    std::vector<std::string> rel_members;
    admin.zrangebyscore(keys.released(), sw::redis::UnboundedInterval<double>{}, std::back_inserter(rel_members));
    REQUIRE(rel_members.size() == 1);
    CHECK(rel_members[0] == entries[1].second.at("event_id"));

    // 5. Running --backfill-contract a second time adds 0 entries (idempotent via dedup).
    {
        std::atomic<int> bf_rc{-1};
        std::thread t([&] { bf_rc = evml::app::backfill_contract(cfg2, "backfill_pool", 10s); });
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return bf_rc.load() != -1; }, 30s));
        t.join();
        REQUIRE(bf_rc.load() == evml::app::kExitOk);
    }
    CHECK(admin.xlen(keys.stream(evml::core::StreamKind::Events)) == 2);

    // 6. The next host startup with the expanded config now passes coverage and reaches Live.
    {
        RunningHost h(cfg2);
        h.start();
        REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
        REQUIRE_FALSE(h.threw.load());
        h.finish();
        CHECK(h.exit_code.load() == 0);
    }
}

// ---- 5-I: Chaos (toxiproxy on provider and Redis links) & Lease-Takeover Stream Integrity ----

TEST_CASE("host: toxiproxy chaos (latency 5s, reset_peer, timeout, bandwidth 0) on provider and Redis links recovers with an exact stream",
          "[integration][chaos]") {
    evml::core::log::init("info");
    const std::string shard = "host_chaos_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-host-chaos-" + random_tag());
    Cleanup cleanup{shard, spill};

    const auto base_cfg = evml::test::load_base_config();
    const std::uint64_t c0 = first_start_block(base_cfg);
    net::io_context mock_ioc;
    NodeState ns;
    ns.safe = c0 + 300;
    ns.finalized = c0 + 250;

    const std::vector<std::uint64_t> log_blocks{c0 + 310, c0 + 320, c0 + 330, c0 + 340, c0 + 350};
    for (const auto n : log_blocks) {
        const std::string bh = hx('a', n);
        ns.blocks[n] = head_json(n, bh, hx('a', n - 1));
        ns.logs.emplace_back(n, cfg_log_json(base_cfg, n, bh, 0));
    }

    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns), "0.0.0.0");
    evml::testing::ToxiHttp px_http(node.port());
    evml::testing::ToxiRedis px_redis;

    ScopedEnv env;
    env.set("EVML_TEST_CHAOS_REDIS_URL", px_redis.url());

    auto toml = host_toml(shard, spill, px_http.url(), 30000, true, 60000, 500);
    replace_once(toml, "redis_url_env      = \"REDIS_URL\"", "redis_url_env      = \"EVML_TEST_CHAOS_REDIS_URL\"");
    replace_once(toml, "request_timeout_ms = 15000", "request_timeout_ms = 800");
    replace_once(toml, "failover_after         = 3", "failover_after         = 100");
    replace_once(toml, "reconcile_rps          = 2", "reconcile_rps          = 100");
    replace_once(toml, "reconcile_burst        = 4", "reconcile_burst        = 100");
    auto cfg = evml::core::Config::parse(toml, "chaos.toml", provider_env(px_http.url(), "chaos-host"));

    RunningHost h(std::move(cfg));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    const auto wait_for = [&](std::chrono::milliseconds d) {
        const auto end = std::chrono::steady_clock::now() + d;
        evml::test::run_until(mock_ioc, [&] { return std::chrono::steady_clock::now() >= end; }, d + 1s);
    };

    // 1. latency = 5000 ms (exceeds 800 ms HTTP RPC timeout) on both provider and Redis links.
    px_http.add_toxic("lat", "latency", {{"latency", 5000}, {"jitter", 0}});
    px_redis.add_toxic("lat", "latency", {{"latency", 5000}, {"jitter", 0}});
    ns.safe = c0 + 315;
    ns.finalized = c0 + 265;
    wait_for(1200ms);
    px_http.remove_toxic("lat");
    px_redis.remove_toxic("lat");
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.status().c_s >= c0 + 315 || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // 2. reset_peer (TCP RST) on both provider and Redis links.
    px_http.add_toxic("rst", "reset_peer", {{"timeout", 0}});
    px_redis.add_toxic("rst", "reset_peer", {{"timeout", 0}});
    ns.safe = c0 + 325;
    ns.finalized = c0 + 275;
    wait_for(600ms);
    px_http.remove_toxic("rst");
    px_redis.remove_toxic("rst");
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.status().c_s >= c0 + 325 || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // 3. timeout (blackhole / hang) on both provider and Redis links.
    px_http.add_toxic("tmo", "timeout", {{"timeout", 0}});
    px_redis.add_toxic("tmo", "timeout", {{"timeout", 0}});
    ns.safe = c0 + 335;
    ns.finalized = c0 + 285;
    wait_for(1200ms);
    px_http.remove_toxic("tmo");
    px_redis.remove_toxic("tmo");
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.status().c_s >= c0 + 335 || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // 4. bandwidth = 0 KB/s on both provider and Redis links.
    px_http.add_toxic("bw0", "bandwidth", {{"rate", 0}});
    px_redis.add_toxic("bw0", "bandwidth", {{"rate", 0}});
    ns.safe = c0 + 360;
    ns.finalized = c0 + 355;
    wait_for(1200ms);
    px_http.remove_toxic("bw0");
    px_redis.remove_toxic("bw0");
    REQUIRE(evml::test::run_until(
        mock_ioc, [&] { return (h.status().c_s >= c0 + 360 && h.status().c_f >= c0 + 355) || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    sw::redis::Redis admin{require_env("REDIS_URL")};
    const evml::sink::Keys keys{shard};
    REQUIRE(evml::test::run_until(
        mock_ioc,
        [&] {
            std::unordered_map<std::string, std::string> ckpt;
            admin.hgetall(keys.ckpt(), std::inserter(ckpt, ckpt.begin()));
            return ckpt.count("safe") && std::stoull(ckpt.at("safe")) >= c0 + 360 &&
                   ckpt.count("finalized") && std::stoull(ckpt.at("finalized")) >= c0 + 355;
        },
        30s));

    h.finish();
    CHECK(h.exit_code.load() == 0);

    using StreamEntry = std::pair<std::string, std::unordered_map<std::string, std::string>>;
    std::vector<StreamEntry> entries;
    admin.xrange(keys.stream(evml::core::StreamKind::Events), "-", "+", std::back_inserter(entries));
    REQUIRE(entries.size() == log_blocks.size());
    for (std::size_t i = 0; i < log_blocks.size(); ++i) {
        CHECK(std::stoull(entries[i].second.at("block_number")) == log_blocks[i]);
        CHECK(entries[i].second.at("type") == "log");
    }
    CHECK(admin.zcard(keys.released()) == 0);
}

TEST_CASE("host: lease takeover under load produces a gap-free, duplicate-free stream and fences the old leader",
          "[integration][chaos]") {
    evml::core::log::init("info");
    const std::string shard = "host_takeover_stream_" + random_tag();
    const fs::path spill_a = fs::temp_directory_path() / ("evml-takeover-a-" + random_tag());
    const fs::path spill_b = fs::temp_directory_path() / ("evml-takeover-b-" + random_tag());
    Cleanup cleanup_a{shard, spill_a};
    Cleanup cleanup_b{shard, spill_b};

    const auto base_cfg = evml::test::load_base_config();
    const std::uint64_t c0 = first_start_block(base_cfg);
    net::io_context mock_ioc;
    NodeState ns;
    ns.safe = c0 + 300;
    ns.finalized = c0 + 250;

    const std::vector<std::uint64_t> log_blocks{c0 + 310, c0 + 320, c0 + 330, c0 + 340};
    for (const auto n : log_blocks) {
        const std::string bh = hx('a', n);
        ns.blocks[n] = head_json(n, bh, hx('a', n - 1));
        ns.logs.emplace_back(n, cfg_log_json(base_cfg, n, bh, 0));
    }

    evml::test::MockHttpServer node(mock_ioc, tunable_node(ns));
    std::vector<evml::test::MockRequest> posts;
    evml::test::MockHttpServer webhook(mock_ioc, [&](const evml::test::MockRequest& r) {
        posts.push_back(r);
        return evml::test::MockReply{.body = "{}"};
    });

    evml::testing::ToxiRedis px_a;
    ScopedEnv env;
    env.set("EVML_TEST_TAKEOVER_REDIS_A", px_a.url());
    env.set("N8N_ALERT_WEBHOOK_URL", webhook.url() + "hook");
    env.set("N8N_ALERT_WEBHOOK_TOKEN", "takeover-test-token");

    auto toml_a = host_toml(shard, spill_a, node.url(), 30000, true, 3000, 500);
    replace_once(toml_a, "redis_url_env      = \"REDIS_URL\"", "redis_url_env      = \"EVML_TEST_TAKEOVER_REDIS_A\"");
    replace_once(toml_a, "reconcile_rps          = 2", "reconcile_rps          = 100");
    replace_once(toml_a, "reconcile_burst        = 4", "reconcile_burst        = 100");
    auto cfg_a = evml::core::Config::parse(toml_a, "takeover_a.toml", provider_env(node.url(), "hostA"));

    auto toml_b = host_toml(shard, spill_b, node.url(), 30000, true, 3000, 500);
    replace_once(toml_b, "reconcile_rps          = 2", "reconcile_rps          = 100");
    replace_once(toml_b, "reconcile_burst        = 4", "reconcile_burst        = 100");
    auto cfg_b = evml::core::Config::parse(toml_b, "takeover_b.toml", provider_env(node.url(), "hostB"));

    RunningHost a(std::move(cfg_a));
    RunningHost b(std::move(cfg_b));

    a.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return a.live() || a.threw.load(); }, 30s));
    REQUIRE_FALSE(a.threw.load());

    b.start();
    evml::test::run_until(mock_ioc, [&] { return b.live() || b.threw.load(); }, 1000ms);
    CHECK_FALSE(b.live());

    // Host A sweeps the first 2 logs (c0 + 310, c0 + 320).
    ns.safe = c0 + 325;
    ns.finalized = c0 + 275;
    sw::redis::Redis admin{require_env("REDIS_URL")};
    const evml::sink::Keys keys{shard};
    REQUIRE(evml::test::run_until(
        mock_ioc,
        [&] {
            std::unordered_map<std::string, std::string> ckpt;
            admin.hgetall(keys.ckpt(), std::inserter(ckpt, ckpt.begin()));
            return ckpt.count("safe") && std::stoull(ckpt.at("safe")) >= c0 + 325 &&
                   admin.xlen(keys.stream(evml::core::StreamKind::Events)) == 2;
        },
        30s));

    // Cut Host A's Redis link while advancing the chain across c0 + 330 and c0 + 340. Host A spills locally while its
    // 3 s lease expires; Host B takes over and sweeps the remaining blocks.
    px_a.disable();
    ns.safe = c0 + 350;
    ns.finalized = c0 + 345;

    REQUIRE(evml::test::run_until(
        mock_ioc, [&] { return (b.live() && b.status().c_s >= c0 + 350 && b.status().c_f >= c0 + 345) || b.threw.load(); }, 30s));
    REQUIRE_FALSE(b.threw.load());
    CHECK(b.leader_events.load() == 1);

    // Reconnect Host A to Redis: its stale epoch token is fenced, its spilled/queued records are discarded, and it
    // demotes cleanly to standby without writing duplicates.
    px_a.enable();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return a.demoted_events.load() >= 1 || a.threw.load(); }, 15s));
    REQUIRE_FALSE(a.threw.load());

    // Wait for Host B to evaluate and post the Leader Changed alert once.
    const auto find_leader_changed = [&]() -> std::optional<boost::json::object> {
        for (const auto& p : posts) {
            const auto obj = boost::json::parse(p.body).as_object();
            if (obj.at("title").as_string() == "Leader Changed" &&
                obj.at("details").as_object().at("new_host").as_string() == "hostB") {
                return obj;
            }
        }
        return std::nullopt;
    };
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return find_leader_changed().has_value() || b.threw.load(); }, 20s));
    const auto lc = find_leader_changed();
    REQUIRE(lc.has_value());
    CHECK(lc->at("details").as_object().at("old_host").as_string() == "hostA");

    a.finish();
    b.finish();
    CHECK(a.exit_code.load() == 0);
    CHECK(b.exit_code.load() == 0);

    using StreamEntry = std::pair<std::string, std::unordered_map<std::string, std::string>>;
    std::vector<StreamEntry> entries;
    admin.xrange(keys.stream(evml::core::StreamKind::Events), "-", "+", std::back_inserter(entries));
    REQUIRE(entries.size() == log_blocks.size());
    for (std::size_t i = 0; i < log_blocks.size(); ++i) {
        CHECK(std::stoull(entries[i].second.at("block_number")) == log_blocks[i]);
        CHECK(entries[i].second.at("type") == "log");
    }
}

// ---- Stage 4a: WS failover ring continuity & safe-reorg re-sweep cross-check watermarks ----

TEST_CASE("host: WS failover preserves the head ring so a forked head from the backup provider walks and re-verifies",
          "[integration][host]") {
    evml::core::log::init("info");
    const std::string shard = "host_ws_failover_fork_" + random_tag();
    const fs::path spill = fs::temp_directory_path() / ("evml-ws-failover-fork-" + random_tag());
    Cleanup cleanup{shard, spill};

    const std::uint64_t c0 = first_start_block(evml::test::load_base_config());
    net::io_context mock_ioc;
    NodeState ns{.safe = c0 + 300, .finalized = c0 + 250};
    evml::test::MockHttpServer http(mock_ioc, tunable_node(ns));

    HeadFeed primary_feed, backup_feed;
    std::atomic<bool> drop_primary{false};
    evml::test::MockWsServer ws_primary(mock_ioc, [&](evml::test::MockWsConn& c) -> net::awaitable<void> {
        if (drop_primary.load()) {
            co_await c.close();
            co_return;
        }
        auto r1 = co_await c.read_req();
        co_await c.send(rpc_reply(r1.id, "\"0x2105\""));
        auto r2 = co_await c.read_req();
        co_await c.send(rpc_reply(r2.id, std::string("\"") + kHeadsSub + "\""));
        auto r3 = co_await c.read_req();
        co_await c.send(rpc_reply(r3.id, std::string("\"") + kLogsSub + "\""));
        while (!drop_primary.load()) {
            if (primary_feed.out.empty()) {
                co_await c.sleep(10ms);
                continue;
            }
            const std::string frame = std::move(primary_feed.out.front());
            primary_feed.out.pop_front();
            co_await c.send(frame);
        }
        co_await c.close();
    });
    evml::test::MockWsServer ws_backup(mock_ioc, head_script(backup_feed));

    const auto env_lookup = [&](const std::string& k) -> std::string {
        if (k.rfind("BASE_RPC_URL", 0) == 0) return http.url();
        if (k == "BASE_WSS_URL_QUICKNODE") return ws_primary.url();
        if (k == "BASE_WSS_URL") return ws_backup.url();
        if (k.rfind("BASE_WSS_URL", 0) == 0) return "ws://127.0.0.1:9/";
        if (k == "EVML_HOST_ID") return "test-host";
        return "";
    };
    RunningHost h(evml::core::Config::parse(host_toml(shard, spill, http.url(), 30000), "ws_failover_fork.toml", env_lookup));
    h.start();
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return h.live() || h.threw.load(); }, 30s));
    REQUIRE_FALSE(h.threw.load());

    // Seed the ring on quicknode (ws_primary) with canonical branch 'a' across c0 + 297 .. c0 + 305.
    for (auto n = c0 + 297; n <= c0 + 305; ++n) {
        primary_feed.out.push_back(head_msg(n, hx('a', n), hx('a', n - 1)));
    }
    REQUIRE(evml::test::run_until(mock_ioc, [&] { return primary_feed.out.empty(); }, 10s));
    evml::test::run_until(mock_ioc, [] { return false; }, 700ms);
    const int before = ns.getlogs_calls;

    // Prepare branch 'b' forking off 'a' at c0 + 298 (below C_s = c0 + 300) for blocks c0 + 299 .. c0 + 305,
    // and queue head c0 + 306 on branch 'b' on the backup WS server (infura).
    for (auto n = c0 + 299; n <= c0 + 305; ++n) {
        ns.blocks[n] = head_json(n, hx('b', n), n == c0 + 299 ? hx('a', c0 + 298) : hx('b', n - 1));
    }
    backup_feed.out.push_back(head_msg(c0 + 306, hx('b', c0 + 306), hx('b', c0 + 305)));

    // Drop quicknode WS so the pool fails over to infura (ws_backup), which immediately delivers head c0 + 306 on 'b'.
    drop_primary = true;
    const bool reverified =
        evml::test::run_until(mock_ioc, [&] { return ns.getlogs_calls > before || h.threw.load(); }, 30s);
    REQUIRE_FALSE(h.threw.load());
    REQUIRE(reverified);

    std::vector<std::uint64_t> expect;
    for (auto n = c0 + 305; n >= c0 + 299; --n) expect.push_back(n);
    CHECK(ns.block_requests == expect);

    REQUIRE(evml::test::run_until(mock_ioc, [&] { return !h.status().reverify_pending && h.live(); }, 15s));
    h.finish();
    CHECK(h.exit_code.load() == 0);
}

TEST_CASE("host: a safe reorg rewinding C_s and re-sweeping previously reconciled blocks does not increment ws_missed",
          "[integration][host]") {
    evml::core::log::init("info");
    HeadRig r;
    r.host.start();
    REQUIRE(r.live());
    r.quiet();

    // Deliver a log at c0 + 310 over both WS and HTTP getLogs, and advance G_s to c0 + 320 so [c0 + 301, c0 + 320] is
    // swept and reconciled once cleanly (ws_missed == 0).
    const std::uint64_t n_fork = r.c0 + 305;
    const std::uint64_t n_kept = r.c0 + 310;
    const std::string h_fork_a = hx('a', n_fork);
    const std::string h_fork_b = hx('b', n_fork);
    const std::string h_kept = hx('a', n_kept);
    const std::string log_fork_a = cfg_log_json(r.host.cfg, n_fork, h_fork_a, 0);
    const std::string log_fork_b = cfg_log_json(r.host.cfg, n_fork, h_fork_b, 1);
    const std::string log_kept = cfg_log_json(r.host.cfg, n_kept, h_kept, 0);

    r.ns.blocks[n_fork] = head_json(n_fork, h_fork_a, hx('a', n_fork - 1));
    r.ns.blocks[n_kept] = head_json(n_kept, h_kept, hx('a', n_kept - 1));
    r.ns.logs.emplace_back(n_fork, log_fork_a);
    r.ns.logs.emplace_back(n_kept, log_kept);
    r.feed.out.push_back(logs_msg(log_fork_a));
    r.feed.out.push_back(logs_msg(log_kept));
    REQUIRE(r.until([&] { return r.feed.out.empty(); }, 10s));
    r.wait(300ms);

    r.ns.safe = r.c0 + 320;
    REQUIRE(r.until([&] { return r.host.status().c_s >= r.c0 + 320 || r.host.threw.load(); }, 30s));
    REQUIRE_FALSE(r.host.threw.load());
    r.quiet();
    CHECK(r.host.ws_missed.load() == 0);
    CHECK(r.host.ws_extra.load() == 0);

    // Trigger a safe reorg at n_fork = c0 + 305 (replacing log_fork_a with log_fork_b) while log_kept at c0 + 310
    // remains in getLogs. reverify_released() rewinds C_s to c0 + 304 and the next safe_step re-sweeps [c0 + 305, c0 + 319].
    r.ns.blocks[n_fork] = head_json(n_fork, h_fork_b, hx('a', n_fork - 1));
    r.ns.logs.clear();
    r.ns.logs.emplace_back(n_fork, log_fork_b);
    r.ns.logs.emplace_back(n_kept, log_kept);
    r.ns.safe = r.c0 + 319;  // safe regression triggers on_safe_reorg_signal -> reverify_released -> rewind_safe(c0 + 304)

    REQUIRE(r.until([&] { return r.host.status().safe_reorgs >= 1 && r.host.status().c_s >= r.c0 + 319; }, 30s));
    REQUIRE_FALSE(r.host.threw.load());
    r.quiet();
    CHECK(r.host.ws_missed.load() == 0);
    CHECK(r.host.ws_extra.load() == 0);

    r.host.finish();
    CHECK(r.host.exit_code.load() == 0);
}
