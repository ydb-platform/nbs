UNITTEST_FOR(contrib/ydb/services/persqueue_v1)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

CFLAGS(
    -DYDB_SDK_USE_STD_STRING
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    persqueue_ut.cpp
    persqueue_common_ut.cpp
    persqueue_compat_ut.cpp
    first_class_src_ids_ut.cpp
    topic_yql_ut.cpp

    test_utils.h
    pq_data_writer.h
    api_test_setup.h
    rate_limiter_test_setup.h
    rate_limiter_test_setup.cpp
    functions_executor_wrapper.h
    functions_executor_wrapper.cpp
    topic_service_ut.cpp
    demo_tx.cpp

    partition_writer_cache_actor_ut.cpp

    pqtablet_mock.cpp
    kqp_mock.cpp
    partition_writer_cache_actor_fixture.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/digest/md5
    contrib/ydb/core/testlib/default
    contrib/ydb/library/aclib
    contrib/ydb/library/persqueue/tests
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/services/persqueue_v1
)

YQL_LAST_ABI_VERSION()

END()
