#include <cstring>
#include <iostream>

#include "core/version.hpp"

int main(int argc, char** argv) {
    if (argc == 2 && std::strcmp(argv[1], "--version") == 0) {
        std::cout << evml::core::version_string() << '\n';
        return 0;
    }
    std::cerr << "usage: evm_listener --version\n";
    return 2;
}
