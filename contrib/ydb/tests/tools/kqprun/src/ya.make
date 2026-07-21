LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    contrib/ydb/core/client/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/kqp/workload_service/actors
    contrib/ydb/core/testlib
    contrib/ydb/core/util
    contrib/ydb/library/aclib
    contrib/ydb/library/aws_init
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/services/persqueue_v1
    contrib/ydb/tests/tools/kqprun/runlib
    contrib/ydb/tests/tools/kqprun/src/proto
    contrib/ydb/library/yql/providers/yt/mkql_dq
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
