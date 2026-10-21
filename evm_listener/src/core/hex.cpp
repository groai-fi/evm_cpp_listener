#include "core/hex.hpp"

#include "core/error.hpp"

namespace evml::core {

namespace {

/// @brief Converts ASCII character to 4-bit hex nibble value (0-15); returns -1 on non-hex characters.
int nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/// @brief Bounds length of string before quoting in error messages to prevent unbounded log amplification.
std::string quoted(std::string_view s) {
    constexpr std::size_t kMax = 80;  // bound the message; inputs can be attacker/provider controlled
    std::string out = "\"";
    out += s.substr(0, kMax);
    if (s.size() > kMax) out += "...";
    out += '"';
    return out;
}

}  // namespace

/// @brief Strictly parses a 0x-prefixed hex quantity string into a 64-bit unsigned integer.
std::uint64_t parse_u64_hex(std::string_view s) {
    // Enforce 0x/0X prefix and at least one trailing hex digit
    if (s.size() < 3 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X')) {
        throw DecodeError("invalid hex quantity (need 0x prefix and >=1 digit): " + quoted(s));
    }
    std::uint64_t v = 0;
    // Iterate over digits after the 0x prefix
    for (const char c : s.substr(2)) {
        const int n = nibble(c);
        if (n < 0) throw DecodeError("invalid hex digit in quantity: " + quoted(s));
        // Guard against 64-bit overflow: shifting left by 4 must not discard bits
        if (v >> 60) throw DecodeError("hex quantity overflows uint64: " + quoted(s));
        v = (v << 4) | static_cast<std::uint64_t>(n);
    }
    return v;
}

/// @brief Formats a 64-bit unsigned integer into minimal lowercase hex notation.
std::string to_hex_u64(std::uint64_t v) {
    static constexpr char kDigits[] = "0123456789abcdef";
    if (v == 0) return "0x0";
    std::string rev;
    // Extract low nibbles in reverse order
    while (v) {
        rev.push_back(kDigits[v & 0xf]);
        v >>= 4;
    }
    // Prefix 0x and reverse to most significant nibble first
    return "0x" + std::string(rev.rbegin(), rev.rend());
}

/// @brief Parses a fixed-size byte array from a strict "0x"-prefixed hex string of length 2 + 2*N.
template <std::size_t N>
FixedBytes<N> FixedBytes<N>::from_hex(std::string_view s) {
    // Validate exact length and 0x prefix
    if (s.size() != 2 + 2 * N || s[0] != '0' || (s[1] != 'x' && s[1] != 'X')) {
        throw DecodeError("expected 0x + " + std::to_string(2 * N) + " hex digits: " + quoted(s));
    }
    FixedBytes<N> out;
    // Parse consecutive pairs of hex characters into single bytes
    for (std::size_t i = 0; i < N; ++i) {
        const int hi = nibble(s[2 + 2 * i]);
        const int lo = nibble(s[3 + 2 * i]);
        if (hi < 0 || lo < 0) throw DecodeError("invalid hex digit: " + quoted(s));
        out.bytes[i] = static_cast<std::uint8_t>((hi << 4) | lo);
    }
    return out;
}

/// @brief Formats fixed bytes into lowercase "0x"-prefixed hex string.
template <std::size_t N>
std::string FixedBytes<N>::to_hex() const {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out = "0x";
    out.reserve(2 + 2 * N);
    // Write high and low nibbles for each byte
    for (const auto b : bytes) {
        out.push_back(kDigits[b >> 4]);
        out.push_back(kDigits[b & 0xf]);
    }
    return out;
}

template struct FixedBytes<32>;
template struct FixedBytes<20>;

}  // namespace evml::core
