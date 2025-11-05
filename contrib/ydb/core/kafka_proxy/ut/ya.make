UNITTEST_FOR(contrib/ydb/core/kafka_proxy)

SIZE(medium)
TIMEOUT(600)

SRCS(
    ut_kafka_functions.cpp
    ut_protocol.cpp
    ut_serialization.cpp
    metarequest_ut.cpp
    port_discovery_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kafka_proxy
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils

)
YQL_LAST_ABI_VERSION()
END()
