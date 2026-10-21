#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "core/config.hpp"

namespace evml::pipeline {

// Generates sorted list of "address:start_block:class" entries representing active contracts.
// Stored in Redis `ckpt.contracts` hash field to detect schema/manifest drift.
std::vector<std::string> contract_manifest(const core::Config& cfg);

// Evaluates whether newly added contracts have historical gaps below current safe cursor C_s.
// Returns list of uncovered contract names; empty if all contracts are covered.
// Watch out: Non-empty return must halt the daemon (exit code 2) to prevent silent event omission (Invariant I1).
std::vector<std::string> uncovered_contracts(const core::Config& cfg, const std::vector<std::string>& stored,
                                             std::uint64_t c_s);

// Calculates initial sweep cursor for a fresh shard without prior Redis checkpoint.
// Clamps cursor to the block immediately preceding earliest contract start_block (or safe gate G_s).
// Watch out: Cursors are inclusive upper bounds of swept blocks, so start_block 100 begins with cursor 99.
std::uint64_t initial_cursor(const core::Config& cfg, std::uint64_t g_s);

}  // namespace evml::pipeline
