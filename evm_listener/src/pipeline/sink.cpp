#include "pipeline/sink.hpp"

#include <stdexcept>

namespace evml::pipeline {

Emission make_emission(const Event& e, std::optional<std::uint64_t> released_block) {
    Emission m;
    m.stream = e.stream;
    m.type = e.type;
    m.event_id = e.event_id;
    m.dedup_key = dedup_key(e);
    m.fields = to_fields(e);
    m.released_block = released_block;
    return m;
}

Emission make_revert(const ReleasedEntry& entry, std::uint64_t emitted_ms) {
    Emission m;
    m.stream = entry.stream;
    m.type = EventType::Revert;
    m.event_id = entry.event_id;
    m.dedup_key = "dedup:" + entry.event_id;
    m.fields = entry.fields;
    bool saw_type = false, saw_emitted = false;
    for (auto& [k, v] : m.fields) {
        if (k == "type") {
            v = to_string(EventType::Revert);
            saw_type = true;
        } else if (k == "emitted_ms") {
            v = std::to_string(emitted_ms);
            saw_emitted = true;
        }
    }
    if (!saw_type || !saw_emitted)
        throw std::logic_error("make_revert: released entry " + entry.event_id + " lacks type/emitted_ms fields");
    return m;
}

ReleasedEntry to_released_entry(const Emission& e) {
    if (!e.released_block) throw std::logic_error("to_released_entry: emission " + e.event_id + " has no released_block");
    return ReleasedEntry{e.event_id, *e.released_block, e.stream, e.fields};
}

}  // namespace evml::pipeline
