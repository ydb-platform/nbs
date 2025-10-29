UNITTEST_FOR(cloud/blockstore/libs/logbroker/topic_api_impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    topic_api_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
