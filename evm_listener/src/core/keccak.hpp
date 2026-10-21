#pragma once

#include <string_view>

#include "core/hex.hpp"

namespace evml::core {

/// @brief Validates canonical EVM event signature format (e.g., "Transfer(address,address,uint256)").
/// Rules:
/// - Exact canonical syntax: EventName(type1,type2,...)
/// - No whitespace or named parameters allowed (e.g. "Transfer(address from)" is invalid).
/// - Balanced parentheses, no trailing characters, no empty components (e.g. "Transfer(uint256,,address)").
/// - Tuples "(uint256,address)" and arrays "uint256[]" are permitted.
/// @warning Throws ConfigError if signature is not strictly canonical.
void validate_event_signature(std::string_view sig);

/// @brief Computes standard 256-bit Keccak (SHA-3 family) hash of byte sequence.
/// @param data Input byte buffer to hash.
/// @return 32-byte digest as Hash32.
Hash32 keccak256(std::string_view data);

/// @brief Computes EVM topic0 hash from a canonical event signature string.
/// @param canonical_signature Canonical event signature (e.g., "Swap(address,uint256,uint256)").
/// @return 32-byte event topic0 hash.
/// @warning Throws ConfigError if signature fails validation.
Hash32 topic0(std::string_view canonical_signature);

}  // namespace evml::core
