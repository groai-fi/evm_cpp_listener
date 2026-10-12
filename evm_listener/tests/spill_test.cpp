#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "sink/spill.hpp"

using evml::sink::Spill;
using evml::sink::SpillError;
using evml::sink::SpillRecord;
using Catch::Matchers::ContainsSubstring;
namespace fs = std::filesystem;

namespace {

// A unique scratch directory removed on scope exit.
struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("evml-spill-test-" + std::to_string(rd()) + std::to_string(rd()));
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

std::size_t segment_files(const fs::path& dir) {
    std::size_t n = 0;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().filename().string().rfind("seg-", 0) == 0) ++n;
    return n;
}

SpillRecord pop_one(Spill& s) {
    auto r = s.peek();
    REQUIRE(r.has_value());
    s.pop();
    return *r;
}

}  // namespace

TEST_CASE("spill: crc32c matches the standard check value", "[unit][spill]") {
    const std::string v = "123456789";
    CHECK(evml::sink::crc32c(v.data(), v.size()) == 0xE3069283u);
}

TEST_CASE("spill: records round-trip in FIFO order, including empty and binary payloads", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20);
    CHECK(s.empty());
    CHECK_FALSE(s.peek().has_value());

    const std::string binary("a\0b\xff\n", 5);
    s.append(1, "first");
    s.append(2, "");
    s.append(3, binary);
    CHECK(s.records() == 3);
    CHECK(s.bytes() == 3 * 9 + 5 + 0 + 5);

    const auto a = pop_one(s);
    CHECK(a.kind == 1);
    CHECK(a.payload == "first");
    const auto b = pop_one(s);
    CHECK(b.kind == 2);
    CHECK(b.payload.empty());
    const auto c = pop_one(s);
    CHECK(c.kind == 3);
    CHECK(c.payload == binary);

    CHECK(s.empty());
    CHECK(s.bytes() == 0);
    CHECK(segment_files(t.path) == 0);  // fully consumed -> deleted
}

TEST_CASE("spill: peek does not consume and pop without peek is an error", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20);
    s.append(7, "x");
    CHECK(s.peek()->payload == "x");
    CHECK(s.peek()->payload == "x");
    CHECK(s.records() == 1);
    s.pop();
    CHECK_THROWS_WITH(s.pop(), ContainsSubstring("pop() without a peeked record"));
}

TEST_CASE("spill: segments roll at the size limit and are deleted once consumed", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20, /*segment_bytes=*/64);
    const std::string payload(20, 'p');  // 29 bytes on disk: two per 64-byte segment
    for (int i = 0; i < 6; ++i) s.append(1, payload + std::to_string(i));
    CHECK(segment_files(t.path) == 3);

    for (int i = 0; i < 6; ++i) {
        CHECK(pop_one(s).payload == payload + std::to_string(i));
        CHECK(segment_files(t.path) == (i < 1 ? 3u : i < 3 ? 2u : i < 5 ? 1u : 0u));
    }
    CHECK(s.empty());
}

TEST_CASE("spill: an oversized record gets a segment of its own and still round-trips", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20, /*segment_bytes=*/32);
    s.append(1, "small");
    s.append(2, std::string(100, 'z'));
    s.append(3, "tail");
    CHECK(pop_one(s).payload == "small");
    CHECK(pop_one(s).payload == std::string(100, 'z'));
    CHECK(pop_one(s).payload == "tail");
}

TEST_CASE("spill: items appended during replay queue behind the backlog, never around it", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20, /*segment_bytes=*/64);
    s.append(1, "a");
    s.append(1, "b");
    CHECK(pop_one(s).payload == "a");
    s.append(1, "c");  // arrives mid-replay
    CHECK(pop_one(s).payload == "b");
    s.append(1, "d");
    CHECK(pop_one(s).payload == "c");
    CHECK(pop_one(s).payload == "d");
    CHECK(s.empty());
}

TEST_CASE("spill: appending after a full drain starts a fresh segment", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20);
    s.append(1, "one");
    CHECK(pop_one(s).payload == "one");
    CHECK(segment_files(t.path) == 0);
    s.append(1, "two");
    CHECK(pop_one(s).payload == "two");
}

TEST_CASE("spill: a corrupted payload is an explicit CRC error, never skipped", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20);
    s.append(1, "payload-to-corrupt");
    {
        std::fstream f(t.path / "seg-0.spill", std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.good());
        f.seekp(9 + 3);  // inside the payload
        f.put('X');
    }
    CHECK_THROWS_AS(s.peek(), SpillError);
    CHECK_THROWS_WITH(s.peek(), ContainsSubstring("CRC mismatch"));
    CHECK(s.records() == 1);  // nothing was consumed
}

TEST_CASE("spill: a truncated segment is an explicit error", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20);
    s.append(1, "will be cut short");
    fs::resize_file(t.path / "seg-0.spill", 12);
    CHECK_THROWS_WITH(s.peek(), ContainsSubstring("short payload read"));
}

TEST_CASE("spill: construction wipes only stale spill segments", "[unit][spill]") {
    TempDir t;
    fs::create_directories(t.path);
    { std::ofstream(t.path / "seg-7.spill") << "stale from a previous process"; }
    { std::ofstream(t.path / "seg-9.spill") << "also stale"; }
    { std::ofstream(t.path / "keep-me.txt") << "unrelated"; }

    Spill s(t.path, 1 << 20);
    CHECK(s.empty());
    CHECK_FALSE(fs::exists(t.path / "seg-7.spill"));
    CHECK_FALSE(fs::exists(t.path / "seg-9.spill"));
    CHECK(fs::exists(t.path / "keep-me.txt"));
}

TEST_CASE("spill: reaching max_bytes is saturated, draining clears it, and append never refuses", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, /*max_bytes=*/100, /*segment_bytes=*/1 << 20);
    const std::string payload(41, 'p');  // 50 bytes on disk
    s.append(1, payload);
    CHECK_FALSE(s.saturated());
    s.append(1, payload);
    CHECK(s.saturated());  // 100 >= 100
    s.append(1, payload);  // the item was already accepted upstream: it must not be dropped
    CHECK(s.records() == 3);
    CHECK(s.saturated());

    pop_one(s);  // 100 bytes left: still at the cap
    CHECK(s.saturated());
    pop_one(s);  // 50 bytes left
    CHECK_FALSE(s.saturated());
}

TEST_CASE("spill: discard_all reports the dropped records, removes the files and stays usable", "[unit][spill]") {
    TempDir t;
    Spill s(t.path, 1 << 20, /*segment_bytes=*/64);
    for (int i = 0; i < 5; ++i) s.append(1, std::string(20, 'x'));
    CHECK(segment_files(t.path) >= 2);

    CHECK(s.discard_all() == 5);
    CHECK(s.empty());
    CHECK(s.bytes() == 0);
    CHECK(segment_files(t.path) == 0);
    CHECK(s.discard_all() == 0);

    s.append(2, "after");
    CHECK(pop_one(s).payload == "after");
}

TEST_CASE("spill: invalid construction arguments fail fast", "[unit][spill]") {
    TempDir t;
    CHECK_THROWS_WITH(Spill(fs::path{}, 100), ContainsSubstring("spill_dir is empty"));
    CHECK_THROWS_WITH(Spill(t.path, 0), ContainsSubstring("max_bytes must be > 0"));
    CHECK_THROWS_WITH(Spill(t.path, 100, 0), ContainsSubstring("segment_bytes must be > 0"));
}
