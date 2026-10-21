#include "sink/spill.hpp"

#include <array>
#include <limits>
#include <system_error>

namespace evml::sink {

namespace {

constexpr std::size_t kHeaderBytes = 9;  // u32 len + u32 crc + u8 kind

const std::array<std::uint32_t, 256>& crc_table() {
    static const std::array<std::uint32_t, 256> table = [] {
        std::array<std::uint32_t, 256> t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1u) ? (c >> 1) ^ 0x82F63B78u : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    return table;
}

void put_u32(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
}

std::uint32_t get_u32(const char* p) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
    return v;
}

bool is_spill_segment(const std::filesystem::path& p) {
    const std::string name = p.filename().string();
    return name.rfind("seg-", 0) == 0 && p.extension() == ".spill";
}

}  // namespace

std::uint32_t crc32c(const void* data, std::size_t len) {
    const auto& t = crc_table();
    const auto* p = static_cast<const unsigned char*>(data);
    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i) crc = t[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    return ~crc;
}

Spill::Spill(std::filesystem::path dir, std::uint64_t max_bytes, std::uint64_t segment_bytes)
    : dir_(std::move(dir)), max_bytes_(max_bytes), segment_bytes_(segment_bytes) {
    if (dir_.empty()) throw SpillError("spill: spill_dir is empty");
    if (max_bytes_ == 0) throw SpillError("spill: max_bytes must be > 0");
    if (segment_bytes_ == 0) throw SpillError("spill: segment_bytes must be > 0");

    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    if (ec) throw SpillError("spill: cannot create " + dir_.string() + ": " + ec.message());

    // Wipe leftovers of an earlier process. Only our own segment files: a mis-set spill_dir must not delete anything else.
    for (const auto& entry : std::filesystem::directory_iterator(dir_, ec)) {
        if (!entry.is_regular_file() || !is_spill_segment(entry.path())) continue;
        std::error_code rm_ec;
        std::filesystem::remove(entry.path(), rm_ec);
        if (rm_ec) throw SpillError("spill: cannot remove stale " + entry.path().string() + ": " + rm_ec.message());
    }
    if (ec) throw SpillError("spill: cannot list " + dir_.string() + ": " + ec.message());
}

Spill::~Spill() {
    // Best-effort cleanup of ephemeral files; a leftover is wiped at the next process start anyway.
    writer_.close();
    reader_.close();
    std::error_code ec;
    for (const auto& s : segments_) std::filesystem::remove(path_of(s.id), ec);
}

std::filesystem::path Spill::path_of(std::uint64_t id) const {
    return dir_ / ("seg-" + std::to_string(id) + ".spill");
}

void Spill::open_tail_segment() {
    writer_.close();
    writer_.clear();
    const std::uint64_t id = next_id_++;
    writer_.open(path_of(id), std::ios::binary | std::ios::trunc);
    if (!writer_.is_open()) throw SpillError("spill: cannot open " + path_of(id).string() + " for writing");
    segments_.push_back(Segment{id, 0, 0});
}

void Spill::remove_segment_file(std::uint64_t id) {
    std::error_code ec;
    std::filesystem::remove(path_of(id), ec);
    if (ec) throw SpillError("spill: cannot remove " + path_of(id).string() + ": " + ec.message());
}

void Spill::append(std::uint8_t kind, std::string_view payload) {
    if (payload.size() > std::numeric_limits<std::uint32_t>::max())
        throw SpillError("spill: payload of " + std::to_string(payload.size()) + " bytes exceeds the u32 length field");

    const std::uint64_t rec = kHeaderBytes + payload.size();
    // Roll over to new segment file if current segment exceeds segment_bytes_.
    if (segments_.empty() || (segments_.back().size > 0 && segments_.back().size + rec > segment_bytes_))
        open_tail_segment();

    std::string buf;
    buf.reserve(rec);
    // On-disk format: [u32 len][u32 crc32c][u8 kind][payload]
    put_u32(buf, static_cast<std::uint32_t>(payload.size()));
    put_u32(buf, 0);  // crc placeholder
    buf.push_back(static_cast<char>(kind));
    buf.append(payload);
    // Compute Castagnoli CRC32C over [kind + payload] and backfill into header.
    const std::uint32_t crc = crc32c(buf.data() + 8, 1 + payload.size());
    for (int i = 0; i < 4; ++i) buf[4 + i] = static_cast<char>((crc >> (8 * i)) & 0xFFu);

    writer_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    writer_.flush();  // to the OS, so the reader (same process) sees it; deliberately no fsync
    if (!writer_.good()) throw SpillError("spill: write to " + path_of(segments_.back().id).string() + " failed");

    segments_.back().size += rec;
    bytes_ += rec;
    ++records_;
}

std::optional<SpillRecord> Spill::peek() {
    if (peeked_) return peeked_;
    if (records_ == 0) return std::nullopt;

    Segment& s = segments_.front();
    if (s.read_off >= s.size)
        throw SpillError("spill: inconsistent state: segment " + std::to_string(s.id) + " is drained but still listed");

    if (reader_id_ != s.id) {
        reader_.close();
        reader_.clear();
        reader_.open(path_of(s.id), std::ios::binary);
        if (!reader_.is_open()) throw SpillError("spill: cannot open " + path_of(s.id).string() + " for reading");
        reader_id_ = s.id;
    }
    reader_.clear();
    reader_.seekg(static_cast<std::streamoff>(s.read_off));

    char header[kHeaderBytes];
    reader_.read(header, static_cast<std::streamsize>(kHeaderBytes));
    if (reader_.gcount() != static_cast<std::streamsize>(kHeaderBytes))
        throw SpillError("spill: short header read in segment " + std::to_string(s.id) + " at offset " + std::to_string(s.read_off));

    const std::uint32_t len = get_u32(header);
    const std::uint32_t crc = get_u32(header + 4);
    if (len > s.size - s.read_off - kHeaderBytes)
        throw SpillError("spill: record length " + std::to_string(len) + " overruns segment " + std::to_string(s.id) +
                         " at offset " + std::to_string(s.read_off));

    std::string body(1 + static_cast<std::size_t>(len), '\0');
    body[0] = header[8];
    if (len > 0) {
        reader_.read(body.data() + 1, static_cast<std::streamsize>(len));
        if (reader_.gcount() != static_cast<std::streamsize>(len))
            throw SpillError("spill: short payload read in segment " + std::to_string(s.id) + " at offset " + std::to_string(s.read_off));
    }
    // Verify CRC32C checksum over kind discriminator byte and payload string.
    if (crc32c(body.data(), body.size()) != crc)
        throw SpillError("spill: CRC mismatch in segment " + std::to_string(s.id) + " at offset " + std::to_string(s.read_off));

    peeked_ = SpillRecord{static_cast<std::uint8_t>(static_cast<unsigned char>(body[0])), body.substr(1)};
    peeked_size_ = kHeaderBytes + len;
    return peeked_;
}

void Spill::pop() {
    if (!peeked_) throw SpillError("spill: pop() without a peeked record");

    Segment& s = segments_.front();
    s.read_off += peeked_size_;
    bytes_ -= peeked_size_;
    --records_;
    peeked_.reset();
    peeked_size_ = 0;

    if (s.read_off < s.size) return;

    // Fully consumed: delete it. If it is also the writer's segment the writer closes; the next append starts afresh.
    // Unlink exhausted segment file from filesystem to release disk space.
    const std::uint64_t id = s.id;
    if (segments_.size() == 1) writer_.close();
    if (reader_id_ == id) {
        reader_.close();
        reader_id_ = ~0ull;
    }
    segments_.erase(segments_.begin());
    remove_segment_file(id);
}

std::uint64_t Spill::discard_all() {
    const std::uint64_t dropped = records_;
    writer_.close();
    reader_.close();
    reader_id_ = ~0ull;
    peeked_.reset();
    peeked_size_ = 0;
    const std::vector<Segment> all = std::move(segments_);
    segments_.clear();
    records_ = 0;
    bytes_ = 0;
    for (const auto& s : all) remove_segment_file(s.id);
    return dropped;
}

}  // namespace evml::sink
