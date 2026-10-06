#pragma once

#include <fstream>
#include <map>
#include <sstream>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include "core/config.hpp"

namespace evml::test {

// config/base.toml parsed with distinctive fake provider URLs (never real credentials, nothing is contacted).
inline core::Config load_base_config() {
    static const std::map<std::string, std::string> env{
        {"BASE_WSS_URL_QUICKNODE", "wss://qn.example/SECRETQN"},
        {"BASE_RPC_URL_QUICKNODE", "https://qn.example/SECRETQN"},
        {"BASE_WSS_URL", "wss://infura.example/ws/v3/SECRETINFURA"},
        {"BASE_RPC_URL", "https://infura.example/v3/SECRETINFURA"},
        {"BASE_WSS_URL_ALCHEMY", "wss://alchemy.example/v2/SECRETALCHEMY"},
        {"BASE_RPC_URL_ALCHEMY", "https://alchemy.example/v2/SECRETALCHEMY"},
        {"BASE_RPC_URL_ANKR", "https://ankr.example/base/SECRETANKR"},
        {"EVML_HOST_ID", "test-host"},
    };
    std::ifstream f(std::string(EVML_CONFIG_DIR) + "/base.toml", std::ios::binary);
    REQUIRE(f.good());
    std::ostringstream ss;
    ss << f.rdbuf();
    return core::Config::parse(ss.str(), "test.toml", [](const std::string& k) {
        const auto it = env.find(k);
        return it == env.end() ? std::string() : it->second;
    });
}

inline core::Hash32 hash_of(std::uint8_t b) {
    core::Hash32 h;
    h.bytes.fill(b);
    return h;
}

}  // namespace evml::test
