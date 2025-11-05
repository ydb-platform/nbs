UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(7)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_table_split_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    contrib/ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/testlib
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/jsonpath
    contrib/ydb/public/lib/experimental
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/public/sdk/cpp/client/ydb_coordination
    contrib/ydb/public/sdk/cpp/client/ydb_export
    contrib/ydb/public/sdk/cpp/client/ydb_extension
    contrib/ydb/public/sdk/cpp/client/ydb_operation
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_monitoring
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
