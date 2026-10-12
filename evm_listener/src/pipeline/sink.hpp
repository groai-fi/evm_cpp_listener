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
class Sink {
public:
    virtual ~Sink() = default;

    // Backpressure (plan 4.6): when true the pipeline must not release or advance a checkpoint.
    virtual bool saturated() const = 0;

    virtual void emit(Emission e) = 0;
    virtual void checkpoint(CheckpointKind kind, std::uint64_t block) = 0;
    // First run only (rev 6, E): writes safe = finalized = c0 and the contracts manifest in one atomic step, so a crash
    // can never leave a partial checkpoint. A no-op if a checkpoint already exists (plan 4.2 ckpt.lua init).
    virtual void init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) = 0;
    // Stores the contracts manifest (ckpt.contracts). Enqueued like a checkpoint: applied after earlier emissions.
    virtual void set_contracts(std::vector<std::string> manifest) = 0;
    // ZREMRANGEBYSCORE released -inf block (plan 4.2 final_trim.lua).
    virtual void final_trim(std::uint64_t block) = 0;

    // nullopt: unavailable right now (see above). A Checkpoint with every field empty is a fresh store.
    virtual boost::asio::awaitable<std::optional<Checkpoint>> load_checkpoint() = 0;
    // Released members with score in [from, to], including those released before a restart. nullopt: unavailable.
    virtual boost::asio::awaitable<std::optional<std::vector<ReleasedEntry>>> released_range(std::uint64_t from,
                                                                                          std::uint64_t to) = 0;
};

Emission make_emission(const Event& e, std::optional<std::uint64_t> released_block);

// The retraction of `entry`: the original fields with type=revert and a fresh emitted_ms.
Emission make_revert(const ReleasedEntry& entry, std::uint64_t emitted_ms);

// The `released` record for a log emission, built from its own fields (what the sink stores for the diff).
ReleasedEntry to_released_entry(const Emission& e);

}  // namespace evml::pipeline
