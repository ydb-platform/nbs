UNITTEST_FOR(cloud/blockstore/libs/logbroker/topic_api_impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

IF (OPENSOURCE)
    # TODO(NBS-4760): fix tests and remove tags
    TAG(
        ya:not_autocheck
        ya:manual
    )
ENDIF()

SRCS(
    topic_api_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
