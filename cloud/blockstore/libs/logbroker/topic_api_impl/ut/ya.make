UNITTEST_FOR(cloud/blockstore/libs/logbroker/topic_api_impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

ADDINCL (
    contrib/ydb/public/sdk/cpp
)

SRCS(
    topic_api_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
