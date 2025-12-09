UNITTEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    contrib/ydb/library/actors/http
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/core/base
    contrib/ydb/core/http_proxy
    contrib/ydb/core/testlib/default
    contrib/ydb/library/aclib
    contrib/ydb/library/persqueue/tests
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/services/ydb
)

SRCS(
    ../kinesis_ut.cpp
    ../ymq_ut.cpp
    inside_ydb_ut.cpp
)

RESOURCE(
    contrib/ydb/core/http_proxy/ut/internal_counters.json internal_counters.json
    contrib/ydb/core/http_proxy/ut/proxy_counters.json proxy_counters.json
)

ENV(INSIDE_YDB="1")

YQL_LAST_ABI_VERSION()

END()
