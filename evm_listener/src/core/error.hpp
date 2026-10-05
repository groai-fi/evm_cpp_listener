#pragma once

#include <stdexcept>
#include <string>

namespace evml::core {

// Invalid/missing configuration or environment. main() maps this to exit code 2.
struct ConfigError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Malformed hex / JSON-RPC payload. Never defaulted: always surfaced with context.
struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

}  // namespace evml::core
