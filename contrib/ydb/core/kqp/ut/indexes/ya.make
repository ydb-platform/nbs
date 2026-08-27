UNITTEST_FOR(contrib/ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_indexes_ut.cpp
    kqp_indexes_multishard_ut.cpp
    kqp_indexes_prefixed_vector_ut.cpp
    kqp_indexes_vector_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/udfs/common/knn
    yql/essentials/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()
