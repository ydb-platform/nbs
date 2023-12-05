UNITTEST_FOR(contrib/ydb/services/persqueue_v1)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    TIMEOUT(600)
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
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/services/persqueue_v1
)

YQL_LAST_ABI_VERSION()

END()
