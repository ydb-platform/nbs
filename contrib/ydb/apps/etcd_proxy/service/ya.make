LIBRARY()

SRCS(
    etcd_base_init.cpp
    etcd_gate.cpp
    etcd_grpc.cpp
    etcd_impl.cpp
    etcd_lease.cpp
    etcd_shared.cpp
    etcd_watch.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/apps/etcd_proxy/proto
    contrib/ydb/library/grpc/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/keyvalue
)

RESOURCE(
    contrib/ydb/apps/etcd_proxy/service/create.sql create.sql
    contrib/ydb/apps/etcd_proxy/service/leases.sql leases.sql
    contrib/ydb/apps/etcd_proxy/service/revision.sql revision.sql
)

END()

RECURSE_FOR_TESTS(
    ut
)
