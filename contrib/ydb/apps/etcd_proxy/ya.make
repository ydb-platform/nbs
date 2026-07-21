PROGRAM()

SRCS(
    main.cpp
    proxy.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/library/grpc/server
    contrib/ydb/apps/etcd_proxy/service
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
