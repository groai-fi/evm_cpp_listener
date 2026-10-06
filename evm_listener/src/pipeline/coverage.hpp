#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "core/config.hpp"

namespace evml::pipeline {

// The contract set whose history the stored cursors cover (rev 5, N2): one "address:start_block:class" entry per
// configured contract, sorted. start_block is the decimal block, or "gate" when unset. Stored in `ckpt.contracts`
// next to the cursors. It is a list rather than a hash so that a mismatch can name the contract.
std::vector<std::string> contract_manifest(const core::Config& cfg);

// Names of contracts whose history below C_s is not covered by `stored` (a manifest written by an earlier run).
// A contract needs history iff it has a numeric start_block <= c_s; it is covered iff `stored` lists the same
// address with a numeric start <= its configured one. Contracts started at "gate" or above C_s are reached by the
// sweeps anyway; removed contracts need nothing. Throws std::runtime_error for a malformed stored entry.
// A non-empty result is fatal at startup (exit 2): run `--backfill-contract <name>` (plan 5.6a).
std::vector<std::string> uncovered_contracts(const core::Config& cfg, const std::vector<std::string>& stored,
                                             std::uint64_t c_s);

// Cursor for a run without a checkpoint (plan 3.2 Recovering): start_policy = gate => g_s; contract_start => the
// block before the earliest numeric start_block (contracts at "gate" begin at g_s). Cursors are inclusive, so a
// start at block 0 yields 0 (block 0 has no logs). Note the cursor is per class, so a contract that starts later
// than another one of its class also gets the earlier blocks swept (extra RPC, never wrong events).
std::uint64_t initial_cursor(const core::Config& cfg, std::uint64_t g_s);

}  // namespace evml::pipeline
