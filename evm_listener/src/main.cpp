#include "app/tools.hpp"

/// @brief Primary daemon and operational CLI entry point for the EVM C++ listener.
///
/// Dispatches argument parsing and command execution to evml::app::run_cli().
///
/// @param argc Number of command-line arguments.
/// @param argv Array of command-line argument strings.
/// @return Process exit code (0 for success, 1 for operational error, 2 for usage/config error).
int main(int argc, char** argv) {
    // Forward command-line execution to the application CLI dispatcher.
    return evml::app::run_cli(argc, argv);
}
