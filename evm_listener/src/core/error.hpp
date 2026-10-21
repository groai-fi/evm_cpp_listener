#pragma once

#include <stdexcept>
#include <string>

namespace evml::core {

/// @brief Fatal configuration or environment error causing service exit with code 2.
struct ConfigError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// @brief Payload decoding or serialization failure with context; never swallowed or defaulted.
struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

}  // namespace evml::core
