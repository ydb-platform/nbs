UNITTEST_FOR(contrib/ydb/core/kafka_proxy)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SIZE(medium)
SRCS(
    kafka_test_client.cpp
    kafka_test_client.h
    ut_kafka_functions.cpp
    ut_protocol.cpp
    ut_serialization.cpp
    metarequest_ut.cpp
    ut_transaction_coordinator.cpp
    ut_transaction_actor.cpp
    ut_produce_actor.cpp
    actors_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kafka_proxy
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

)
YQL_LAST_ABI_VERSION()
END()
