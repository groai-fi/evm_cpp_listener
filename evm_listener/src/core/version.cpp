#include "core/version.hpp"

#include <string>

namespace evml::core {

/// @brief Formats and caches version banner string.
const char* version_string() {
    // Lazily construct formatted version banner once and cache across calls
    static const std::string s =
        std::string("evm_listener ") + kSemver + " (" + kGitDescribe + ")";
    return s.c_str();
}

}  // namespace evml::core
