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

// base.toml adapted for a single-host run on this machine: lease off, private shard and spill dir, loopback ops port,
// fast tag polls, and the credential-free public provider URL pointed at `http_url`.
std::string host_toml(const std::string& shard, const fs::path& spill, const std::string& http_url,
                      std::uint32_t process_stall_ms) {
    auto t = read_base_toml();
    replace_once(t, "id      = \"base_eurc_usdc\"", "id      = \"" + shard + "\"");
    replace_once(t, "enabled  = true", "enabled  = false");
    replace_once(t, "safe_poll_ms       = 4000", "safe_poll_ms       = 100");
    replace_once(t, "finalized_poll_ms  = 12000", "finalized_poll_ms  = 100");
    replace_once(t, "process_stall_ms     = 30000", "process_stall_ms     = " + std::to_string(process_stall_ms));
    replace_once(t, "spill_dir          = \"/tmp/evml-spill\"", "spill_dir          = \"" + spill.string() + "\"");
    replace_once(t, "http_listen             = \"[::]:9464\"", "http_listen             = \"127.0.0.1:0\"");
    replace_once(t, "\"https://mainnet.base.org\"", "\"" + http_url + "\"");
    return t;
}

// Every provider env var of base.toml: HTTP -> `http_url`, WS -> a port nobody listens on (validated, never served).
evml::core::EnvLookup provider_env(const std::string& http_url) {
    return [http_url](const std::string& k) -> std::string {
        if (k.rfind("BASE_RPC_URL", 0) == 0) return http_url;
        if (k.rfind("BASE_WSS_URL", 0) == 0) return "ws://127.0.0.1:9/";
        if (k == "EVML_HOST_ID") return "test-host";
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

}  // namespace

TEST_CASE("host: [lease] enabled = true is refused with the usage exit code until 5-D2", "[unit]") {
    const auto cfg = evml::test::load_base_config();  // base.toml ships with the lease enabled
    REQUIRE(cfg.lease.enabled);
    std::atomic<bool> stop{false};
    evml::app::HostOptions opt;
    opt.stop = &stop;
    CHECK(evml::app::run_host(cfg, opt) == evml::app::kExitUsage);
}

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
