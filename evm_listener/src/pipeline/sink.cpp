#include "pipeline/sink.hpp"

#include <stdexcept>

namespace evml::pipeline {

// Constructs an Emission payload from an Event, configuring dedup keys and released tracking.
Emission make_emission(const Event& e, std::optional<std::uint64_t> released_block) {
    Emission m;
    m.stream = e.stream;
    m.type = e.type;
    m.event_id = e.event_id;
    m.dedup_key = dedup_key(e);
    m.fields = to_fields(e);
    m.released_block = released_block; // Set for safe-class events to record in released ZSET
    return m;
}

// Invariant I6: Constructs a revert emission retaining all original before-image fields of the retracted log.
Emission make_revert(const ReleasedEntry& entry, std::uint64_t emitted_ms) {
    Emission m;
    m.stream = entry.stream;
    m.type = EventType::Revert;
    m.event_id = entry.event_id;
    m.dedup_key = "dedup:" + entry.event_id;
    m.fields = entry.fields; // Invariant I6: exact copy of original fields
    bool saw_type = false, saw_emitted = false;
    for (auto& [k, v] : m.fields) {
        if (k == "type") {
            v = to_string(EventType::Revert); // Mutate type to 'revert'
            saw_type = true;
        } else if (k == "emitted_ms") {
            v = std::to_string(emitted_ms);   // Update emitted timestamp to current time
            saw_emitted = true;
        }
    }
    if (!saw_type || !saw_emitted)
        throw std::logic_error("make_revert: released entry " + entry.event_id + " lacks type/emitted_ms fields");
    return m;
}

// Converts an Emission into a ReleasedEntry for tracking in Redis ZSET during safe sweeps.
ReleasedEntry to_released_entry(const Emission& e) {
    if (!e.released_block) throw std::logic_error("to_released_entry: emission " + e.event_id + " has no released_block");
    return ReleasedEntry{e.event_id, *e.released_block, e.stream, e.fields};
}

}  // namespace evml::pipeline
