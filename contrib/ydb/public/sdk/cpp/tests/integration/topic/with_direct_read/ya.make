GTEST(topic_direct_read_it)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

REQUIREMENTS(ram:32 cpu:4)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/tests/integration/topic/setup
    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils
)

YQL_LAST_ABI_VERSION()

ENV(PQ_EXPERIMENTAL_DIRECT_READ="1")

CFLAGS(
    -DPQ_EXPERIMENTAL_DIRECT_READ=1
)

SRCS(
    ../basic_usage_it.cpp
    ../describe_topic_it.cpp
    ../direct_read_it.cpp
    ../local_partition_it.cpp
    ../topic_to_table_it.cpp
)

END()
