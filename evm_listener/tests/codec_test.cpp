#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include "core/config.hpp"
#include "core/error.hpp"
#include "rpc/codec.hpp"

using namespace evml;
using namespace evml::rpc;
using Catch::Matchers::ContainsSubstring;
namespace bj = boost::json;

#ifndef EVML_FIXTURE_DIR
#error "EVML_FIXTURE_DIR must point at evm_listener/tests/fixtures"
#endif

namespace {

std::string fixture(const std::string& name) {
    std::ifstream f(std::string(EVML_FIXTURE_DIR) + "/" + name + ".json", std::ios::binary);
    REQUIRE(f.good());
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

std::string mutate(std::string text, const std::string& from, const std::string& to) {
    const auto pos = text.find(from);
    REQUIRE(pos != std::string::npos);
    text.replace(pos, from.size(), to);
    return text;
}

const Response& as_response(const Message& m) {
    REQUIRE(std::holds_alternative<Response>(m));
    return std::get<Response>(m);
}

const Notification& as_notification(const Message& m) {
    REQUIRE(std::holds_alternative<Notification>(m));
    return std::get<Notification>(m);
}

const char* kPool50 = "0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7";
const char* kSwapTopic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

}  // namespace

TEST_CASE("newHeads notification decodes into Head", "[unit][codec]") {
    Decoder d;
    const auto m = d.decode(fixture("ws_new_head"));
    const auto& n = as_notification(m);
    CHECK(n.subscription == "0x9ce59a13059e417087c02d3236a0b1cc");
    REQUIRE(std::holds_alternative<Head>(n.payload));
    const auto& h = std::get<Head>(n.payload);
    CHECK(h.number == 0x31cbb7fULL);
    CHECK(h.timestamp == 0x6ac3d3e1ULL);
    CHECK(h.hash.to_hex() == "0x951b17bf34f30959309c76f2bbc3ea1df58465fde0579b940c95cf093c99e876");
    CHECK(h.parent_hash.to_hex() == "0xde7023c1aa788fec333b45aa09553c71216827c9b4b4ec2fbf9646e3ffbc6bd0");
}

TEST_CASE("log notification decodes into Log (real Base swap)", "[unit][codec]") {
    Decoder d;
    const auto m = d.decode(fixture("ws_log"));
    const auto& n = as_notification(m);
    CHECK(n.subscription == "0x4a8a4c0d8a3b4e5f9d7c1b2a3e4f5a6b");
    REQUIRE(std::holds_alternative<Log>(n.payload));
    const auto& l = std::get<Log>(n.payload);
    CHECK(l.address.to_hex() == kPool50);
    REQUIRE(l.topics.size() == 3);
    CHECK(l.topics[0].to_hex() == kSwapTopic);
    CHECK(l.block_number == 0x31cafc8ULL);
    CHECK(l.tx_index == 0x38);
    CHECK(l.log_index == 0x107);
    CHECK(l.block_hash.to_hex() == "0xafc52eaecd48e82bc9522ce18fd0db9323faa48f34c5a4516d05202537c804c4");
    CHECK(l.tx_hash.to_hex() == "0x372b4d249d6a1ff45cf725faaaa0fd823383ccffb55af5319f85a4a3382bbd8c");
    CHECK(l.data.rfind("0xffffffff", 0) == 0);
    CHECK(l.data.size() == 2 + 5 * 64);  // 5 words: int256,int256,uint160,uint128,int24
    CHECK_FALSE(l.removed);
}

TEST_CASE("removed:true log notification is surfaced, not dropped", "[unit][codec]") {
    Decoder d;
    const auto m = d.decode(fixture("ws_log_removed"));
    const auto& l = std::get<Log>(as_notification(m).payload);
    CHECK(l.removed);
}

TEST_CASE("simple responses and typed result extractors", "[unit][codec]") {
    Decoder d;
    const auto bn = d.decode(fixture("resp_block_number"));
    const auto& bnr = as_response(bn);
    REQUIRE(bnr.ok);
    CHECK(bnr.id == std::optional<std::uint64_t>(1));
    CHECK(d.parse_quantity(bnr.result_raw) > 0x31cafc8ULL);

    const auto cid = d.decode(fixture("resp_chain_id"));
    CHECK(d.parse_quantity(as_response(cid).result_raw) == 8453);
    CHECK(as_response(cid).result_raw == "\"0x2105\"");  // exact JSON text, no trailing whitespace

    const auto sub = d.decode(fixture("resp_subscribe"));
    CHECK(d.parse_string(as_response(sub).result_raw) == "0x9ce59a13059e417087c02d3236a0b1cc");
}

TEST_CASE("eth_getBlockByNumber: real block and null result", "[unit][codec]") {
    Decoder d;
    const auto blk = d.decode(fixture("resp_block_latest"));
    const auto head = d.parse_block(as_response(blk).result_raw);
    REQUIRE(head.has_value());
    CHECK(head->number > 0x31cafc8ULL);
    CHECK(head->timestamp > 0x6ac3bc73ULL);

    const auto null_blk = d.decode(fixture("resp_block_null"));
    REQUIRE(as_response(null_blk).ok);
    CHECK_FALSE(d.parse_block(as_response(null_blk).result_raw).has_value());
}

TEST_CASE("eth_getLogs responses: 4 real swaps, and empty", "[unit][codec]") {
    Decoder d;
    const auto m = d.decode(fixture("resp_get_logs"));
    const auto logs = d.parse_logs(as_response(m).result_raw);
    REQUIRE(logs.size() == 4);
    for (const auto& l : logs) {
        CHECK(l.address.to_hex() == kPool50);
        REQUIRE_FALSE(l.topics.empty());
        CHECK(l.topics[0].to_hex() == kSwapTopic);
        CHECK_FALSE(l.removed);
    }
    CHECK(logs[0].block_number <= logs[3].block_number);

    const auto e = d.decode(fixture("resp_get_logs_empty"));
    CHECK(d.parse_logs(as_response(e).result_raw).empty());
}

TEST_CASE("provider error shapes observed 2026-10-05 decode as RpcError", "[unit][codec]") {
    Decoder d;
    const auto infura = as_response(d.decode(fixture("err_infura_32603")));
    CHECK_FALSE(infura.ok);
    CHECK(infura.error.code == -32603);
    CHECK(infura.error.message == "service temporarily unavailable");

    const auto qn = as_response(d.decode(fixture("err_quicknode_32615")));
    CHECK(qn.error.code == -32615);
    CHECK_THAT(qn.error.message, ContainsSubstring("eth_getLogs is limited to a 5 range"));

    const auto pub = as_response(d.decode(fixture("err_public_32614")));
    CHECK(pub.error.code == -32614);
    CHECK_THAT(pub.error.message, ContainsSubstring("limited to a 500 range"));

    const auto with_data = as_response(d.decode(fixture("err_with_data")));
    CHECK(with_data.error.code == 3);
    CHECK(with_data.error.data_raw == "\"0x08c379a0\"");

    const auto null_id = as_response(d.decode(fixture("err_null_id")));
    CHECK_FALSE(null_id.id.has_value());
    CHECK_FALSE(null_id.ok);
    CHECK(null_id.error.code == -32700);
}

TEST_CASE("try_parse_error_body: both observed shapes, and non-errors", "[unit][codec]") {
    Decoder d;
    const auto alchemy = d.try_parse_error_body(fixture("err_alchemy_429"));  // bare {code,message}
    REQUIRE(alchemy.has_value());
    CHECK(alchemy->code == 429);
    CHECK_THAT(alchemy->message, ContainsSubstring("Monthly capacity limit exceeded"));

    const auto wrapped = d.try_parse_error_body(fixture("err_quicknode_32615"));
    REQUIRE(wrapped.has_value());
    CHECK(wrapped->code == -32615);

    CHECK_FALSE(d.try_parse_error_body("not json").has_value());
    CHECK_FALSE(d.try_parse_error_body("<html>Cloudflare 1010</html>").has_value());
    CHECK_FALSE(d.try_parse_error_body("{}").has_value());
    CHECK_FALSE(d.try_parse_error_body(R"({"jsonrpc":"2.0","id":1,"result":"0x1"})").has_value());
    CHECK_FALSE(d.try_parse_error_body("[1,2]").has_value());
    CHECK_FALSE(d.try_parse_error_body(R"({"code":"429","message":"x"})").has_value());  // code must be an integer
}

TEST_CASE("strict decoding: a missing or malformed field is a DecodeError naming it", "[unit][codec]") {
    Decoder d;
    const std::string log = fixture("ws_log");
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"removed\": false", "\"removedX\": false")), ContainsSubstring("field \"removed\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"removed\": false", "\"removed\": \"no\"")), ContainsSubstring("field \"removed\": expected boolean"));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"blockNumber\": \"0x31cafc8\"", "\"blockNumber\": \"0xzz\"")), ContainsSubstring("field \"blockNumber\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"blockNumber\": \"0x31cafc8\"", "\"blockNumber\": \"31cafc8\"")), ContainsSubstring("field \"blockNumber\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"logIndex\": \"0x107\"", "\"logIndex\": \"0x10000000000000000\"")), ContainsSubstring("overflows"));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"transactionIndex\": \"0x38\"", "\"transactionIndexX\": \"0x38\"")), ContainsSubstring("field \"transactionIndex\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7", "0xe846373c")), ContainsSubstring("field \"address\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"data\": \"0xffff", "\"data\": \"0xfff")), ContainsSubstring("field \"data\""));  // odd length
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"data\": \"0xffff", "\"data\": \"0xzzff")), ContainsSubstring("field \"data\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"topics\": [", "\"topics\": [\"0x00\", ")), ContainsSubstring("field \"topics\""));
    CHECK_THROWS_WITH(d.decode(mutate(log, "\"topics\": [", std::string("\"topics\": [") + "\"0x" + std::string(64, '1') + "\",\"0x" +
                                                               std::string(64, '2') + "\",")),
                      ContainsSubstring("more than 4 topics"));

    const std::string head = fixture("ws_new_head");
    CHECK_THROWS_WITH(d.decode(mutate(head, "\"parentHash\"", "\"parentHashX\"")), ContainsSubstring("field \"parentHash\""));
    CHECK_THROWS_WITH(d.decode(mutate(head, "\"timestamp\": \"0x6ac3d3e1\"", "\"timestamp\": 1")), ContainsSubstring("field \"timestamp\": expected string"));
}

TEST_CASE("structural decode errors", "[unit][codec]") {
    Decoder d;
    CHECK_THROWS_AS(d.decode("[1,2]"), core::DecodeError);
    CHECK_THROWS_AS(d.decode("\"str\""), core::DecodeError);
    CHECK_THROWS_AS(d.decode("{"), core::DecodeError);
    CHECK_THROWS_AS(d.decode(""), core::DecodeError);
    CHECK_THROWS_WITH(d.decode("{}"), ContainsSubstring("neither \"id\" nor \"method\""));
    CHECK_THROWS_WITH(d.decode(R"({"jsonrpc":"2.0","method":"eth_other","params":{}})"), ContainsSubstring("unexpected notification method"));
    CHECK_THROWS_WITH(d.decode(R"({"jsonrpc":"2.0","id":"abc","result":"0x1"})"), ContainsSubstring("field \"id\""));
    CHECK_THROWS_WITH(d.decode(R"({"jsonrpc":"2.0","id":1})"), ContainsSubstring("field \"result\""));
    CHECK_THROWS_WITH(d.decode(R"({"jsonrpc":"2.0","id":1,"error":{"message":"x"}})"), ContainsSubstring("field \"code\""));
    CHECK_THROWS_AS(d.parse_quantity("\"nothex\""), core::DecodeError);
    CHECK_THROWS_AS(d.parse_logs("{}"), core::DecodeError);
    CHECK_THROWS_AS(d.parse_block("[]"), core::DecodeError);
}

TEST_CASE("a Decoder survives reuse across many messages, including after errors", "[unit][codec]") {
    Decoder d;
    const std::string head = fixture("ws_new_head");
    const std::string log = fixture("ws_log");
    for (int i = 0; i < 200; ++i) {
        CHECK(std::holds_alternative<Head>(as_notification(d.decode(head)).payload));
        CHECK_THROWS_AS(d.decode("{\"id\":"), core::DecodeError);
        CHECK(std::holds_alternative<Log>(as_notification(d.decode(log)).payload));
    }
}

TEST_CASE("request builders produce valid JSON-RPC with monotonic ids", "[unit][codec]") {
    const auto bn = bj::parse(build_block_number(1)).as_object();
    CHECK(bn.at("jsonrpc").as_string() == "2.0");
    CHECK(bn.at("id").as_int64() == 1);
    CHECK(bn.at("method").as_string() == "eth_blockNumber");
    CHECK(bn.at("params").as_array().empty());

    CHECK(bj::parse(build_chain_id(2)).as_object().at("method").as_string() == "eth_chainId");

    const auto sub = bj::parse(build_subscribe(3, "newHeads")).as_object();
    CHECK(sub.at("method").as_string() == "eth_subscribe");
    CHECK(sub.at("params").as_array().at(0).as_string() == "newHeads");

    const auto unsub = bj::parse(build_unsubscribe(4, "0xabc")).as_object();
    CHECK(unsub.at("method").as_string() == "eth_unsubscribe");
    CHECK(unsub.at("params").as_array().at(0).as_string() == "0xabc");

    const auto blk = bj::parse(build_get_block_by_number(5, "safe", false)).as_object();
    CHECK(blk.at("method").as_string() == "eth_getBlockByNumber");
    CHECK(blk.at("params").as_array().at(0).as_string() == "safe");
    CHECK(blk.at("params").as_array().at(1).as_bool() == false);

    const auto code = bj::parse(build_get_code(6, Address20::from_hex("0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7"), "0x10")).as_object();
    CHECK(code.at("method").as_string() == "eth_getCode");
    CHECK(code.at("params").as_array().at(0).as_string() == kPool50);  // lowercase
    CHECK(code.at("params").as_array().at(1).as_string() == "0x10");
}

TEST_CASE("build_get_logs shapes the filter and validates arguments", "[unit][codec]") {
    core::LogFilter f;
    f.addresses.push_back(Address20::from_hex("0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7"));
    f.topic0s.push_back(Hash32::from_hex(kSwapTopic));

    const auto o = bj::parse(build_get_logs(9, 16, 32, f)).as_object();
    CHECK(o.at("method").as_string() == "eth_getLogs");
    CHECK(o.at("id").as_int64() == 9);
    const auto& p = o.at("params").as_array().at(0).as_object();
    CHECK(p.at("fromBlock").as_string() == "0x10");
    CHECK(p.at("toBlock").as_string() == "0x20");
    REQUIRE(p.at("address").as_array().size() == 1);
    CHECK(p.at("address").as_array().at(0).as_string() == kPool50);  // lowercase
    REQUIRE(p.at("topics").as_array().size() == 1);                  // [[t0...]]
    CHECK(p.at("topics").as_array().at(0).as_array().at(0).as_string() == kSwapTopic);

    CHECK_THROWS_AS(build_get_logs(1, 33, 32, f), std::invalid_argument);
    CHECK_THROWS_AS(build_get_logs(1, 1, 2, core::LogFilter{}), std::invalid_argument);
    core::LogFilter no_topics;
    no_topics.addresses = f.addresses;
    CHECK_THROWS_AS(build_get_logs(1, 1, 2, no_topics), std::invalid_argument);
}

TEST_CASE("codec monkey: random truncations byte flips and type mutations never crash or UB",
          "[unit][codec][property]") {
    const std::vector<std::string> fixtures{
        fixture("ws_new_head"),
        fixture("ws_log"),
        fixture("resp_block_latest"),
        fixture("resp_get_logs"),
        fixture("err_alchemy_429"),
    };
    const std::vector<char> corrupt_bytes{'\0', '\xff', '{', '}', '[', ']', '"', ':', '0', 'x', 'Z', '\n'};
    const std::vector<std::string> target_tokens{"\"0x", "\"result\"", "\"params\"", "\"topics\"",
                                                 "\"number\"", "\"hash\"", "\"blockNumber\""};
    const std::vector<std::string> replacements{"null", "12345", "[]", "{}", "\"0x123g\"", "\"\""};

    Decoder d;
    std::mt19937_64 rng(0xC0DEC001ULL);
    std::size_t decode_errors = 0;

    for (std::size_t iter = 0; iter < 1500; ++iter) {
        INFO("seed 0xC0DEC001 iter " << iter);
        std::string mutated = fixtures[rng() % fixtures.size()];
        const std::size_t mode = rng() % 3;

        if (mode == 0 && !mutated.empty()) {
            // 1. Random prefix truncation
            mutated.resize(rng() % mutated.size());
        } else if (mode == 1 && !mutated.empty()) {
            // 2. Random byte overwrite (1..4 positions)
            const std::size_t count = 1 + (rng() % 4);
            for (std::size_t k = 0; k < count; ++k) {
                mutated[rng() % mutated.size()] = corrupt_bytes[rng() % corrupt_bytes.size()];
            }
        } else {
            // 3. Targeted JSON token mutation
            const std::string& tok = target_tokens[rng() % target_tokens.size()];
            std::vector<std::size_t> positions;
            for (std::size_t pos = mutated.find(tok, 0); pos != std::string::npos; pos = mutated.find(tok, pos + 1)) {
                positions.push_back(pos);
            }
            if (!positions.empty()) {
                const std::size_t pos = positions[rng() % positions.size()];
                const std::string& rep = replacements[rng() % replacements.size()];
                mutated.replace(pos, tok.size(), rep);
            } else if (!mutated.empty()) {
                mutated.resize(rng() % mutated.size());
            }
        }

        // try_parse_error_body must never throw any exception
        CHECK_NOTHROW(d.try_parse_error_body(mutated));

        bool unexpected_exception = false;
        try {
            const Message msg = d.decode(mutated);
            if (const auto* r = std::get_if<Response>(&msg); r && r->ok) {
                try {
                    (void)d.parse_logs(r->result_raw);
                } catch (const core::DecodeError&) {
                    ++decode_errors;
                }
                try {
                    (void)d.parse_block(r->result_raw);
                } catch (const core::DecodeError&) {
                    ++decode_errors;
                }
                try {
                    (void)d.parse_quantity(r->result_raw);
                } catch (const core::DecodeError&) {
                    ++decode_errors;
                }
                try {
                    (void)d.parse_string(r->result_raw);
                } catch (const core::DecodeError&) {
                    ++decode_errors;
                }
            }
        } catch (const core::DecodeError&) {
            ++decode_errors;
        } catch (...) {
            unexpected_exception = true;
        }
        REQUIRE_FALSE(unexpected_exception);
    }
    CHECK(decode_errors > 0);
}
