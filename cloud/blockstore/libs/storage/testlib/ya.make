LIBRARY(blockstore-libs-storage-testlib)

SRCS(
    counters_printer.cpp
    diagnostics.cpp
    disk_agent_mock.cpp
    disk_registry_proxy_mock.cpp
    root_kms_key_provider_mock.cpp
    service_client.cpp
    ss_proxy_client.cpp
    ss_proxy_mock.cpp
    test_env.cpp
    test_env_state.cpp
    test_executor.cpp
    test_runtime.cpp
    test_tablet.cpp
    ut_helpers.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery
    cloud/blockstore/libs/endpoints
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/service
    cloud/blockstore/libs/storage/ss_proxy
    cloud/blockstore/libs/storage/stats_service
    cloud/blockstore/libs/storage/undelivered
    cloud/blockstore/libs/storage/volume
    cloud/blockstore/libs/storage/volume_balancer
    cloud/blockstore/libs/storage/volume_proxy
    cloud/blockstore/libs/ydbstats
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/api
    cloud/storage/core/libs/hive_proxy

    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/kqp
    contrib/ydb/core/mind
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/mind/hive
    contrib/ydb/core/protos
    contrib/ydb/core/security
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tablet_flat/test/libs/table
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
    contrib/ydb/core/tx/coordinator
    contrib/ydb/core/tx/mediator
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_allocator
    contrib/ydb/core/tx/tx_proxy

    contrib/ydb/library/actors/core
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
