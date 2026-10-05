#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace evml::core {

// Strict quantity parsing: requires "0x", >=1 hex digit, rejects overflow of uint64.
// Throws DecodeError. Leading zeros are accepted if the value still fits.
std::uint64_t parse_u64_hex(std::string_view s);

// Minimal "0x.." quantity ("0x0" for zero), lowercase.
std::string to_hex_u64(std::uint64_t v);

// Fixed-size byte string (Hash32, Address20). Strict "0x" + exactly 2*N hex digits.
template <std::size_t N>
struct FixedBytes {
    std::array<std::uint8_t, N> bytes{};

    static FixedBytes from_hex(std::string_view s);  // throws DecodeError
    std::string to_hex() const;                      // "0x" + lowercase

    friend bool operator==(const FixedBytes&, const FixedBytes&) = default;
};

using Hash32 = FixedBytes<32>;
using Address20 = FixedBytes<20>;

extern template struct FixedBytes<32>;
extern template struct FixedBytes<20>;

}  // namespace evml::core

namespace std {
template <std::size_t N>
struct hash<evml::core::FixedBytes<N>> {
    std::size_t operator()(const evml::core::FixedBytes<N>& v) const noexcept {
        // FNV-1a over the bytes: inputs are cryptographic hashes/addresses, no adversarial concern here.
        std::size_t h = 1469598103934665603ULL;
        for (const auto b : v.bytes) {
            h ^= b;
            h *= 1099511628211ULL;
        }
        return h;
    }
};
}  // namespace std
