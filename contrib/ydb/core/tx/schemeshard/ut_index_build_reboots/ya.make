UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(100)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)

    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/public/sdk/cpp/src/client/table
)

SRCS(
    ut_fulltext_index_build_last_key_ack.cpp
    ut_fulltext_index_build_reboots.cpp
    ut_index_build_reboots.cpp
    ut_json_index_build_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
