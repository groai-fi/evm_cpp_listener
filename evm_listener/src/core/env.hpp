#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace evml::core::env {

/// @brief Walks up filesystem directories starting from `start` to root, loading `.env`, `.env.local.dev`, `.env.local`.
/// Invariants:
/// - Priority: Earlier-discovered (nearer to current working directory) files take precedence over parent files.
/// - Non-Overwriting: Variables already defined in process environment are preserved (overwrite = 0).
/// - Parsing: Handles CRLF line breaks, `export ` prefixes, single/double quotes, and inline ` #` comments.
/// @param start Starting directory path to begin ascending traversal.
void load_dotenv_from(const std::filesystem::path& start);

/// @brief Loads dotenv configuration from current working directory up to root.
/// @warning Throws ConfigError if current directory cannot be queried.
void load_dotenv();

/// @brief Retrieves environment variable value, or fallback default if unset or empty.
/// @param key Environment variable name.
/// @param default_val Default fallback string if variable is unset.
/// @return String value.
std::string get(const std::string& key, const std::string& default_val = "");

/// @brief Retrieves required environment variable value, failing fast if unset or empty.
/// @param key Environment variable name.
/// @return Non-empty string value.
/// @warning Throws ConfigError if variable is missing or empty.
std::string require(const std::string& key);

/// @brief Masks sensitive credentials, access tokens, API keys, and userinfo in RPC URLs for safe logging.
/// Transformations:
/// - wss://base-mainnet.g.alchemy.com/v2/secretKey -> wss://base-mainnet.g.alchemy.com/****
/// - http://alice:password@localhost:8545/ -> http://****@localhost:8545/
/// @param url Raw endpoint URL.
/// @return Redacted URL string, or "<invalid-url>" if malformed.
std::string redact(std::string_view url);

}  // namespace evml::core::env
