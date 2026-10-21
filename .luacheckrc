-- .luacheckrc: luacheck config for the Redis Lua scripts (single source of truth; tools/lint_lua.sh loads it explicitly).
--
-- Redis runs scripts in a sandboxed Lua 5.1 that exposes LESS than luacheck's "lua51" std (no print, require, io,
-- debug, ...; os has only clock). The "redis" std below is the exact global table of the Redis 8.4.7 `redis` compose
-- service (pinned to prod), dumped from _G via EVAL. Anything not listed here is nil inside Redis.
-- Re-dump and update when bumping the Redis major version.

stds.redis = {
    read_globals = {
        -- EVAL inputs
        "KEYS", "ARGV",

        -- Base functions
        "_VERSION", "assert", "collectgarbage", "error", "gcinfo", "getmetatable", "ipairs", "load", "loadstring",
        "next", "pairs", "pcall", "rawequal", "rawget", "rawset", "select", "setmetatable", "tonumber", "tostring",
        "type", "unpack", "xpcall",

        -- Standard libraries (Redis subset)
        coroutine = { fields = { "create", "resume", "running", "status", "wrap", "yield" } },
        math = { fields = {
            "abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "cosh", "deg", "exp", "floor", "fmod", "frexp",
            "huge", "ldexp", "log", "log10", "max", "min", "mod", "modf", "pi", "pow", "rad", "random", "randomseed",
            "sin", "sinh", "sqrt", "tan", "tanh",
        } },
        os = { fields = { "clock" } },
        string = { fields = {
            "byte", "char", "dump", "find", "format", "gfind", "gmatch", "gsub", "len", "lower", "match", "rep",
            "reverse", "sub", "upper",
        } },
        table = { fields = { "concat", "foreach", "foreachi", "getn", "insert", "maxn", "remove", "setn", "sort" } },

        -- Redis-bundled libraries
        bit = { fields = {
            "arshift", "band", "bnot", "bor", "bswap", "bxor", "lshift", "rol", "ror", "rshift", "tobit", "tohex",
        } },
        cjson = { fields = {
            "_NAME", "_VERSION", "decode", "decode_array_with_array_mt", "decode_invalid_numbers", "decode_max_depth",
            "encode", "encode_invalid_numbers", "encode_keep_buffer", "encode_max_depth", "encode_number_precision",
            "encode_sparse_array", "new", "null",
        } },
        cmsgpack = { fields = {
            "_COPYRIGHT", "_DESCRIPTION", "_NAME", "_VERSION", "pack", "unpack", "unpack_limit", "unpack_one",
        } },
        struct = { fields = { "pack", "size", "unpack" } },

        -- Redis API (EVAL context; FUNCTION-only APIs such as register_function are intentionally absent)
        redis = { fields = {
            "LOG_DEBUG", "LOG_NOTICE", "LOG_VERBOSE", "LOG_WARNING", "REDIS_VERSION", "REDIS_VERSION_NUM",
            "REPL_ALL", "REPL_AOF", "REPL_NONE", "REPL_REPLICA", "REPL_SLAVE", "acl_check_cmd", "breakpoint",
            "call", "debug", "error_reply", "log", "pcall", "replicate_commands", "set_repl", "setresp", "sha1hex",
            "status_reply",
        } },
    },
}

std = "redis"
