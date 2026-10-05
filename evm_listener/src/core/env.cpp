#include "core/env.hpp"

#include <cstdlib>
#include <fstream>
#include <system_error>
#include <vector>

#include "core/error.hpp"

namespace evml::core::env {

namespace {

constexpr const char* kWhitespace = " \t\r\n";

std::string trim(std::string s) {
    const auto first = s.find_first_not_of(kWhitespace);
    if (first == std::string::npos) return {};
    const auto last = s.find_last_not_of(kWhitespace);
    return s.substr(first, last - first + 1);
}

std::string parse_value(std::string value) {
    value = trim(std::move(value));
    if (value.size() >= 2 && (value.front() == '"' || value.front() == '\'')) {
        const char quote = value.front();
        const auto close = value.find(quote, 1);
        if (close != std::string::npos) return value.substr(1, close - 1);
        return value.substr(1);  // unterminated quote: keep the remainder verbatim
    }
    // Unquoted: a ` #` starts an inline comment (URLs never contain whitespace-then-hash).
    for (std::size_t i = 1; i < value.size(); ++i) {
        if (value[i] == '#' && (value[i - 1] == ' ' || value[i - 1] == '\t')) {
            return trim(value.substr(0, i));
        }
    }
    return value;
}

void load_file(const std::filesystem::path& path) {
    std::ifstream file(path);
    if (!file) throw ConfigError("cannot read env file: " + path.string());
    std::string line;
    while (std::getline(file, line)) {
        line = trim(std::move(line));  // strips the CR of CRLF files
        if (line.empty() || line.front() == '#') continue;
        if (line.rfind("export ", 0) == 0) line = trim(line.substr(7));
        const auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        const std::string key = trim(line.substr(0, eq));
        if (key.empty()) continue;
        const std::string value = parse_value(line.substr(eq + 1));
        setenv(key.c_str(), value.c_str(), /*overwrite=*/0);
    }
}

}  // namespace

void load_dotenv_from(const std::filesystem::path& start) {
    namespace fs = std::filesystem;
    static const std::vector<std::string> kCandidates = {".env", ".env.local.dev", ".env.local"};
    fs::path current = start;
    while (!current.empty()) {
        for (const auto& name : kCandidates) {
            std::error_code ec;
            const fs::path candidate = current / name;
            if (fs::is_regular_file(candidate, ec)) load_file(candidate);
        }
        const fs::path parent = current.parent_path();
        if (parent == current) break;  // reached the root
        current = parent;
    }
}

void load_dotenv() {
    std::error_code ec;
    const auto cwd = std::filesystem::current_path(ec);
    if (ec) throw ConfigError("cannot determine current directory: " + ec.message());
    load_dotenv_from(cwd);
}

std::string get(const std::string& key, const std::string& default_val) {
    const char* v = std::getenv(key.c_str());
    return v ? std::string(v) : default_val;
}

std::string require(const std::string& key) {
    const char* v = std::getenv(key.c_str());
    if (!v || *v == '\0') throw ConfigError("missing env: " + key);
    return std::string(v);
}

std::string redact(std::string_view url) {
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string_view::npos || scheme_end == 0) return "<invalid-url>";
    const std::size_t authority_start = scheme_end + 3;
    const auto path_start = url.find_first_of("/?#", authority_start);
    std::string_view authority = url.substr(authority_start, path_start == std::string_view::npos
                                                                 ? std::string_view::npos
                                                                 : path_start - authority_start);
    if (authority.empty()) return "<invalid-url>";
    std::string out(url.substr(0, authority_start));
    if (const auto at = authority.rfind('@'); at != std::string_view::npos) {
        out += "****@";
        authority.remove_prefix(at + 1);
    }
    out += authority;
    if (path_start != std::string_view::npos) {
        const auto rest = url.substr(path_start);
        if (rest != "/") out += "/****";
    }
    return out;
}

}  // namespace evml::core::env
