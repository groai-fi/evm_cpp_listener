#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace evml::core::env {

// Walks up from `start` to the filesystem root, loading `.env`, `.env.local.dev`, `.env.local`
// (in that order per directory). Never overwrites variables already in the process environment.
// Handles CRLF files, `export ` prefixes, quotes, and ` #` inline comments on unquoted values.
void load_dotenv_from(const std::filesystem::path& start);

// load_dotenv_from(current_path()); throws ConfigError if the cwd cannot be determined.
void load_dotenv();

// Value of `key`, or `default_val` when unset.
std::string get(const std::string& key, const std::string& default_val = "");

// Value of `key`; throws ConfigError("missing env: KEY") when unset or empty.
std::string require(const std::string& key);

// Masks everything after the host so endpoint URLs are safe to log:
//   wss://base-mainnet.g.alchemy.com/v2/<key>  ->  wss://base-mainnet.g.alchemy.com/****
// Userinfo (user:pass@) is masked too. Input that is not scheme://authority... yields "<invalid-url>".
std::string redact(std::string_view url);

}  // namespace evml::core::env
