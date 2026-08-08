UNITTEST_FOR(contrib/ydb/services/persqueue_v1)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    persqueue_new_schemecache_ut.cpp
    persqueue_common_new_schemecache_ut.cpp
    ut/api_test_setup.h
    ut/test_utils.h
    ut/pq_data_writer.h
    ut/rate_limiter_test_setup.h
    ut/rate_limiter_test_setup.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/library/persqueue/tests
    contrib/ydb/core/testlib/default
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/services/persqueue_v1
)

YQL_LAST_ABI_VERSION()

END()
