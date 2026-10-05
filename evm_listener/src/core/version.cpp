#include "core/version.hpp"

#include <string>

namespace evml::core {

const char* version_string() {
    static const std::string s =
        std::string("evm_listener ") + kSemver + " (" + kGitDescribe + ")";
    return s.c_str();
}

}  // namespace evml::core
