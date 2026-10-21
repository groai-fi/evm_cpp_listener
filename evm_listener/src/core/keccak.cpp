#include "core/keccak.hpp"

#include <cctype>
#include <cstring>
#include <string>

#include <ethash/keccak.hpp>

#include "core/error.hpp"

namespace evml::core {

/// @brief Validates event signature syntax against strict Ethereum ABI specifications.
void validate_event_signature(std::string_view sig) {
    const auto fail = [&](const char* why) {
        throw ConfigError(std::string("non-canonical event signature (") + why + "): \"" + std::string(sig) + "\"");
    };
    // Ensure signature is not empty
    if (sig.empty()) fail("empty");
    // Verify each character against allowed set (alnum, _, (), [], comma)
    for (const char c : sig) {
        // Disallow whitespace and parameter names
        if (std::isspace(static_cast<unsigned char>(c))) fail("whitespace/param names not allowed");
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '(' || c == ')' || c == ',' ||
              c == '[' || c == ']')) {
            fail("illegal character");
        }
    }
    // Verify opening parenthesis exists and is preceded by identifier
    const auto open = sig.find('(');
    if (open == std::string_view::npos || open == 0) fail("missing event name or '('");
    int depth = 0;
    // Scan parameter list to verify paren balance and syntax
    for (std::size_t i = open; i < sig.size(); ++i) {
        const char c = sig[i];
        if (c == '(') ++depth;
        if (c == ')' && --depth < 0) fail("unbalanced parentheses");
        // Ensure no trailing characters exist after final closing paren
        if (depth == 0 && i + 1 != sig.size()) fail("trailing characters after closing ')'");
        // Check for empty parameter types (e.g., ",," or "(," or ",)")
        if (i + 1 < sig.size()) {
            const char n = sig[i + 1];
            if ((c == ',' && (n == ',' || n == ')')) || (c == '(' && n == ',')) fail("empty component");
        }
    }
    if (depth != 0) fail("unbalanced parentheses");
}

/// @brief Computes 32-byte Keccak-256 digest using ethash library.
Hash32 keccak256(std::string_view data) {
    // Hash raw data buffer
    const auto h = ethash::keccak256(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
    Hash32 out;
    // Copy 32-byte hash result into fixed buffer
    std::memcpy(out.bytes.data(), h.bytes, 32);
    return out;
}

/// @brief Validates event signature and computes its topic0 Keccak hash.
Hash32 topic0(std::string_view canonical_signature) {
    // Validate canonical ABI format before hashing
    validate_event_signature(canonical_signature);
    return keccak256(canonical_signature);
}

}  // namespace evml::core
