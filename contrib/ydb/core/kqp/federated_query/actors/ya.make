LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/protos
    contrib/ydb/core/util
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()
