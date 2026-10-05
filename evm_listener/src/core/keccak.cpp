#include "core/keccak.hpp"

#include <cctype>
#include <cstring>
#include <string>

#include <ethash/keccak.hpp>

#include "core/error.hpp"

namespace evml::core {

void validate_event_signature(std::string_view sig) {
    const auto fail = [&](const char* why) {
        throw ConfigError(std::string("non-canonical event signature (") + why + "): \"" + std::string(sig) + "\"");
    };
    if (sig.empty()) fail("empty");
    for (const char c : sig) {
        if (std::isspace(static_cast<unsigned char>(c))) fail("whitespace/param names not allowed");
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '(' || c == ')' || c == ',' ||
              c == '[' || c == ']')) {
            fail("illegal character");
        }
    }
    const auto open = sig.find('(');
    if (open == std::string_view::npos || open == 0) fail("missing event name or '('");
    int depth = 0;
    for (std::size_t i = open; i < sig.size(); ++i) {
        const char c = sig[i];
        if (c == '(') ++depth;
        if (c == ')' && --depth < 0) fail("unbalanced parentheses");
        if (depth == 0 && i + 1 != sig.size()) fail("trailing characters after closing ')'");
        if (i + 1 < sig.size()) {
            const char n = sig[i + 1];
            if ((c == ',' && (n == ',' || n == ')')) || (c == '(' && n == ',')) fail("empty component");
        }
    }
    if (depth != 0) fail("unbalanced parentheses");
}

Hash32 keccak256(std::string_view data) {
    const auto h = ethash::keccak256(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
    Hash32 out;
    std::memcpy(out.bytes.data(), h.bytes, 32);
    return out;
}

Hash32 topic0(std::string_view canonical_signature) {
    validate_event_signature(canonical_signature);
    return keccak256(canonical_signature);
}

}  // namespace evml::core
