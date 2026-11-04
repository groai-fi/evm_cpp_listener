#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>

#include "core/error.hpp"

namespace evml::core {

/// @brief Non-owning view over contiguous read-only bytes.
using BytesView = std::span<const std::uint8_t>;

/// @brief Non-throwing operational parser for 0x-prefixed hex quantity strings into uint64_t.
[[nodiscard]] Result<std::uint64_t> try_parse_u64_hex(std::string_view s);

/// @brief Strictly parses a 0x-prefixed hex quantity string into a 64-bit unsigned integer.
/// @param s Hex string (must start with "0x" or "0X" and contain 1 to 16 hex digits).
/// @return Parsed uint64_t integer value.
/// @warning Throws DecodeError if prefix is missing, non-hex characters present, or value overflows 64 bits.
[[nodiscard]] std::uint64_t parse_u64_hex(std::string_view s);

/// @brief Formats a 64-bit unsigned integer into minimal lowercase hex notation (e.g. 0 -> "0x0", 16 -> "0x10").
/// @param v Unsigned integer value.
/// @return Minimal "0x"-prefixed lowercase hex string.
[[nodiscard]] std::string to_hex_u64(std::uint64_t v);

/// @brief Fixed-size byte array representing cryptographic Ethereum primitives (hashes, addresses).
/// Invariants:
/// - Exact length: Hex representations require strict "0x" + 2*N hex digits.
/// - Value semantics: Equality and FNV-1a hash based on exact byte content.
template <std::size_t N>
struct FixedBytes {
    std::array<std::uint8_t, N> bytes{};

    /// @brief Non-throwing operational parser for fixed-size byte array from "0x"-prefixed hex string.
    [[nodiscard]] static Result<FixedBytes> try_from_hex(std::string_view s);
    /// @brief Parses a fixed-size byte array from a strict "0x"-prefixed hex string of length 2 + 2*N.
    /// @throws DecodeError on length mismatch or non-hex characters.
    [[nodiscard]] static FixedBytes from_hex(std::string_view s);
    /// @brief Formats fixed bytes into lowercase "0x"-prefixed hex string of length 2 + 2*N.
    [[nodiscard]] std::string to_hex() const;

    friend constexpr bool operator==(const FixedBytes&, const FixedBytes&) noexcept = default;
};

/// @brief Standard 32-byte cryptographic hash (block hash, transaction hash, event topic).
using Hash32 = FixedBytes<32>;
/// @brief Standard 20-byte Ethereum account or contract address.
using Address20 = FixedBytes<20>;

extern template struct FixedBytes<32>;
extern template struct FixedBytes<20>;

}  // namespace evml::core

namespace std {
/// @brief Hash specialization for FixedBytes implementing 64-bit FNV-1a.
template <std::size_t N>
struct hash<evml::core::FixedBytes<N>> {
    [[nodiscard]] std::size_t operator()(const evml::core::FixedBytes<N>& v) const noexcept {
        // FNV-1a over raw bytes: cryptographic inputs eliminate hash collision vulnerabilities
        std::size_t h = 1469598103934665603ULL;
        for (const auto b : v.bytes) {
            h ^= b;
            h *= 1099511628211ULL;
        }
        return h;
    }
};
}  // namespace std
