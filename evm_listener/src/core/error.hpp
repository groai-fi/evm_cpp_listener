#pragma once

#include <boost/system/result.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace evml::core {

/// @brief Lightweight operational error value for non-throwing Result<T> hot paths.
struct Error {
    std::string message;

    Error() = default;
    explicit Error(std::string msg) noexcept : message(std::move(msg)) {}
    explicit Error(std::string_view msg) : message(msg) {}
    explicit Error(const char* msg) : message(msg) {}

    [[nodiscard]] const std::string& what() const noexcept { return message; }
    friend constexpr bool operator==(const Error&, const Error&) noexcept = default;
};

/// @brief ADL hook required by boost::system::result<T, Error>::value().
[[noreturn]] inline void throw_exception_from_error(const Error& e, const boost::source_location&) {
    throw std::runtime_error(e.message);
}

/// @brief Non-throwing operational result type wrapping either a value T or an error E.
template <class T, class E = Error>
using Result = boost::system::result<T, E>;

/// @brief Fatal configuration or environment error causing service exit with code 2.
struct ConfigError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// @brief Payload decoding or serialization failure with context; never swallowed or defaulted.
struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

}  // namespace evml::core
