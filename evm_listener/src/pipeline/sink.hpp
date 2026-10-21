#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include "core/config.hpp"
#include "pipeline/event.hpp"

namespace evml::pipeline {

// What the pipeline hands to the sink for one stream entry (plan 4.2 emit.lua inputs).
struct Emission {
    core::StreamKind stream{core::StreamKind::Events};
    EventType type{EventType::Log};
    std::string event_id;
    std::string dedup_key;                     // "dedup:{event_id}"; its value in Redis is the last `type`
    std::vector<Field> fields;                 // schema v=1, flat
    std::optional<std::uint64_t> released_block;  // set for safe-class log events: tracked in `released` (plan 3.5)
};

// A safe-class event that was released above C_f and is still waiting for the final-sweep diff. It keeps the
// original fields so that a revert can carry them (plan 3.8: "a revert carries the fields of the log it retracts").
struct ReleasedEntry {
    std::string event_id;
    std::uint64_t block_number{};
    core::StreamKind stream{core::StreamKind::Events};
    std::vector<Field> fields;
};

enum class CheckpointKind { Safe, Finalized };

struct Checkpoint {
    std::optional<std::uint64_t> safe;
    std::optional<std::uint64_t> finalized;
    // Sorted "address:start_block:class" entries the cursors cover (rev 5, N2); nullopt if never written.
    std::optional<std::vector<std::string>> contracts;
};

// The pipeline's view of the Phase 4 sink. Writes never block the io thread: they enqueue and return (acks come
// back asynchronously and are the sink's business). Reads are awaitable because they are Redis round-trips.
// Order is preserved: a checkpoint enqueued after emissions is applied after them (plan 4.2 ckpt.lua).
//
// Reads return nullopt when the sink cannot answer (rev 6, C/A): it is degraded or replaying, or the connection
// failed. A read never returns a partial or stale answer: a released set that misses queued emissions would make the
// final-sweep diff skip a vanished log, so the caller must pause and ask again later.
// Abstract interface isolating pipeline state machine from Redis transport, Lua execution, and disk spill WAL.
// Concurrency model: writes enqueue asynchronously and never block the Asio event loop; reads are awaitable.
// Invariants enforced: Invariant I3 (Monotonic Checkpoints), Invariant I4 (Atomic Dedup), Invariant I6 (Revert Fidelity).
class Sink {
public:
    virtual ~Sink() = default;

    // Backpressure probe (plan 4.6): returns true if queue or spill buffer is full.
    // Watch out: Pipeline must immediately pause sweeps when saturated() returns true.
    virtual bool saturated() const = 0;

    // Read availability probe: returns false when sink is disconnected, degraded, or replaying spill WAL.
    // Watch out: Final sweep and reverification should pause rather than burn RPC calls when readable() is false.
    virtual bool readable() const = 0;

    // Enqueues an event emission to the stream; writes dedup key atomically via Lua.
    virtual void emit(Emission e) = 0;

    // Enqueues a checkpoint advance for safe or finalized cursor (applied strictly after preceding emits).
    virtual void checkpoint(CheckpointKind kind, std::uint64_t block) = 0;

    // Atomically initializes safe, finalized, and contracts checkpoint on a fresh deployment (rev 6, E).
    // Watch out: Executed via Lua to ensure zero possibility of partial checkpoints on crash.
    virtual void init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) = 0;

    // Enqueues updated contract manifest (ckpt.contracts); serialized after inflight emissions.
    virtual void set_contracts(std::vector<std::string> manifest) = 0;

    // Trims released tracking ZSET entries with score <= block (plan 4.2 final_trim.lua).
    virtual void final_trim(std::uint64_t block) = 0;

    // Asynchronously fetches checkpoint hash from Redis (nullopt if Redis is unreachable).
    virtual boost::asio::awaitable<std::optional<Checkpoint>> load_checkpoint() = 0;

    // Fetches released safe-class entries with block numbers in [from, to] for final sweep diffing.
    // Watch out: Returns nullopt if sink is degraded or replaying; caller must wait and retry.
    virtual boost::asio::awaitable<std::optional<std::vector<ReleasedEntry>>> released_range(std::uint64_t from,
                                                                                          std::uint64_t to) = 0;
};

Emission make_emission(const Event& e, std::optional<std::uint64_t> released_block);

// The retraction of `entry`: the original fields with type=revert and a fresh emitted_ms.
Emission make_revert(const ReleasedEntry& entry, std::uint64_t emitted_ms);

// The `released` record for a log emission, built from its own fields (what the sink stores for the diff).
ReleasedEntry to_released_entry(const Emission& e);

}  // namespace evml::pipeline
