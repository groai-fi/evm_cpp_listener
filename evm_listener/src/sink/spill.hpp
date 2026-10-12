#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace evml::sink {

// Raised for any spill I/O failure or on-disk inconsistency (CRC mismatch, short read, bad length). Never swallowed:
// a spill that cannot be trusted must stop the sink loudly, not be skipped over (plan 4.5).
struct SpillError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct SpillRecord {
    std::uint8_t kind{};
    std::string payload;
};

// CRC-32C (Castagnoli), exposed for tests.
std::uint32_t crc32c(const void* data, std::size_t len);

// Process-scoped FIFO on ephemeral disk (plan 4.5, rev 6 D1). Items the sink could not hand to Redis wait here and
// replay, in order, after the outage, within the SAME process. It is wiped at construction, never replayed by a later
// process: after a restart, recovery re-sweeps from the Redis checkpoint instead.
//
// On-disk record: [u32 len][u32 crc32c][u8 kind][payload], little endian, crc over kind+payload, `len` = payload size.
// Segments are `seg-<n>.spill`, rolled at `segment_bytes`; a fully consumed segment is deleted. There is no fsync, no
// torn-tail handling and no ack-cursor file: nothing here has to survive the process.
//
// NOT thread-safe: owned and used by the sink thread only.
class Spill {
public:
    static constexpr std::uint64_t kDefaultSegmentBytes = 64ull * 1024 * 1024;

    // Creates `dir` if needed and deletes any `seg-*.spill` files left by an earlier process. Other files in `dir` are
    // never touched (a mis-set spill_dir must not delete unrelated data). Throws SpillError on an empty path or I/O error.
    Spill(std::filesystem::path dir, std::uint64_t max_bytes, std::uint64_t segment_bytes = kDefaultSegmentBytes);
    ~Spill();
    Spill(const Spill&) = delete;
    Spill& operator=(const Spill&) = delete;

    // Appends at the tail. Never refuses: an item the sink already accepted must not be dropped; `saturated()` is how
    // the pipeline is told to stop producing (plan 4.6).
    void append(std::uint8_t kind, std::string_view payload);

    // The oldest unconsumed record; nullopt when empty. Does not consume: call pop() once it was delivered.
    std::optional<SpillRecord> peek();
    void pop();

    bool empty() const { return records_ == 0; }
    std::uint64_t records() const { return records_; }
    std::uint64_t bytes() const { return bytes_; }
    bool saturated() const { return bytes_ >= max_bytes_; }

    // Drops everything unconsumed and returns how many records were discarded (SinkFenced path, plan 4.8).
    std::uint64_t discard_all();

private:
    struct Segment {
        std::uint64_t id{};
        std::uint64_t size{};       // bytes written
        std::uint64_t read_off{};   // bytes consumed
    };

    std::filesystem::path path_of(std::uint64_t id) const;
    void open_tail_segment();
    void remove_segment_file(std::uint64_t id);

    std::filesystem::path dir_;
    std::uint64_t max_bytes_;
    std::uint64_t segment_bytes_;

    std::vector<Segment> segments_;  // oldest first; back() is the writer's segment
    std::uint64_t next_id_{0};
    std::ofstream writer_;           // open iff segments_ is non-empty
    std::ifstream reader_;           // open on segments_.front() while it has unread data
    std::uint64_t reader_id_{~0ull};

    std::optional<SpillRecord> peeked_;
    std::uint64_t peeked_size_{0};   // on-disk size of the peeked record

    std::uint64_t records_{0};
    std::uint64_t bytes_{0};
};

}  // namespace evml::sink
