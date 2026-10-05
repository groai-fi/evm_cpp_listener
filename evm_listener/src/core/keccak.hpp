#pragma once

#include <string_view>

#include "core/hex.hpp"

namespace evml::core {

// Throws ConfigError unless `sig` is a canonical event signature: `Name(type,type,...)`
// with no whitespace (which also rules out named params), balanced parens, no empty components.
// Tuples `(uint256,address)` and arrays `uint256[]` are allowed.
void validate_event_signature(std::string_view sig);

Hash32 keccak256(std::string_view data);

// keccak256 of a validated canonical signature. Throws ConfigError if not canonical.
Hash32 topic0(std::string_view canonical_signature);

}  // namespace evml::core
