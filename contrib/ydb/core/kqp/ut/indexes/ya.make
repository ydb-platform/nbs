UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_indexes_multishard_ut.cpp
    kqp_indexes_ut.cpp
    kqp_stream_indexes_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/udfs/common/knn
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    common
    compact
    fulltext
    hybrid
    json
    prefixed_vector
    vector
)
