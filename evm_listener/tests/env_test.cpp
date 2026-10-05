#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "core/env.hpp"
#include "core/error.hpp"

using namespace evml::core;
namespace fs = std::filesystem;

namespace {

// Unique temp tree per test; also unsets the variables it used so tests stay isolated.
struct TempTree {
    fs::path root;
    std::vector<std::string> vars;

    TempTree() {
        root = fs::temp_directory_path() / ("evml_env_test_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(counter()++));
        fs::create_directories(root / "a" / "b");
    }
    ~TempTree() {
        for (const auto& v : vars) unsetenv(v.c_str());
        std::error_code ec;
        fs::remove_all(root, ec);
    }
    void write(const fs::path& rel, const std::string& content) {
        std::ofstream f(root / rel, std::ios::binary);
        f << content;
    }
    static int& counter() {
        static int c = 0;
        return c;
    }
};

}  // namespace

TEST_CASE("load_dotenv strips CRLF so values carry no trailing CR", "[unit][env]") {
    TempTree t;
    t.vars = {"EVML_T_CRLF", "EVML_T_CRLF2"};
    t.write(".env", "EVML_T_CRLF=wss://host/ws/v3/abc\r\nEVML_T_CRLF2=plain\r\n");
    env::load_dotenv_from(t.root);
    CHECK(env::get("EVML_T_CRLF") == "wss://host/ws/v3/abc");
    CHECK(env::get("EVML_T_CRLF2") == "plain");
}

TEST_CASE("load_dotenv never overwrites an existing variable", "[unit][env]") {
    TempTree t;
    t.vars = {"EVML_T_KEEP"};
    setenv("EVML_T_KEEP", "from_process", 1);
    t.write(".env", "EVML_T_KEEP=from_file\n");
    env::load_dotenv_from(t.root);
    CHECK(env::get("EVML_T_KEEP") == "from_process");
}

TEST_CASE("load_dotenv walks up parent directories; nearer file wins", "[unit][env]") {
    TempTree t;
    t.vars = {"EVML_T_UP", "EVML_T_NEAR"};
    t.write(".env", "EVML_T_UP=root\nEVML_T_NEAR=root\n");
    t.write("a/.env", "EVML_T_NEAR=near\n");
    env::load_dotenv_from(t.root / "a" / "b");
    CHECK(env::get("EVML_T_UP") == "root");
    CHECK(env::get("EVML_T_NEAR") == "near");
}

TEST_CASE("load_dotenv syntax: comments, quotes, export, inline comments, '=' in value", "[unit][env]") {
    TempTree t;
    t.vars = {"EVML_T_Q1", "EVML_T_Q2", "EVML_T_EXP", "EVML_T_INL", "EVML_T_EQ", "EVML_T_URLHASH"};
    t.write(".env",
            "# full line comment\n"
            "\n"
            "EVML_T_Q1=\"double quoted # not a comment\"\n"
            "EVML_T_Q2='single'\n"
            "export EVML_T_EXP=exported\n"
            "EVML_T_INL=value   # trailing comment\n"
            "EVML_T_EQ=https://h/p?a=b&c=d\n"
            "EVML_T_URLHASH=https://h/p#frag\n");
    env::load_dotenv_from(t.root);
    CHECK(env::get("EVML_T_Q1") == "double quoted # not a comment");
    CHECK(env::get("EVML_T_Q2") == "single");
    CHECK(env::get("EVML_T_EXP") == "exported");
    CHECK(env::get("EVML_T_INL") == "value");
    CHECK(env::get("EVML_T_EQ") == "https://h/p?a=b&c=d");
    CHECK(env::get("EVML_T_URLHASH") == "https://h/p#frag");  // '#' without preceding space is data
}

TEST_CASE("env::require throws ConfigError naming the variable", "[unit][env]") {
    unsetenv("EVML_T_MISSING");
    try {
        env::require("EVML_T_MISSING");
        FAIL("expected ConfigError");
    } catch (const ConfigError& e) {
        CHECK(std::string(e.what()) == "missing env: EVML_T_MISSING");
    }
    setenv("EVML_T_EMPTY", "", 1);
    CHECK_THROWS_AS(env::require("EVML_T_EMPTY"), ConfigError);
    unsetenv("EVML_T_EMPTY");
}

TEST_CASE("redact masks path/query/userinfo but keeps scheme and host", "[unit][env]") {
    CHECK(env::redact("wss://base-mainnet.g.alchemy.com/v2/SECRETKEY") == "wss://base-mainnet.g.alchemy.com/****");
    CHECK(env::redact("https://base-mainnet.infura.io/v3/PROJECTID") == "https://base-mainnet.infura.io/****");
    CHECK(env::redact("https://rpc.ankr.com/base/KEY?x=1") == "https://rpc.ankr.com/****");
    CHECK(env::redact("https://x.base-mainnet.quiknode.pro:8443/TOKEN/") == "https://x.base-mainnet.quiknode.pro:8443/****");
    CHECK(env::redact("https://user:pw@host.example/path") == "https://****@host.example/****");
    CHECK(env::redact("https://host.example?token=abc") == "https://host.example/****");
    CHECK(env::redact("https://mainnet.base.org") == "https://mainnet.base.org");  // nothing to hide
    CHECK(env::redact("not a url") == "<invalid-url>");
    CHECK(env::redact("https://") == "<invalid-url>");
    for (const char* u : {"wss://h/v2/SECRETKEY", "https://h/v3/SECRETKEY", "https://h?k=SECRETKEY",
                          "https://SECRETKEY@h/x"}) {
        CHECK(env::redact(u).find("SECRETKEY") == std::string::npos);
    }
}
