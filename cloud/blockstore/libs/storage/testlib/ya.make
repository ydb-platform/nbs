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

    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blockstore/core
    ydb/core/client/minikql_compile
    ydb/core/kqp
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/mind/hive
    ydb/core/protos
    ydb/core/security
    ydb/core/tablet_flat
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    ydb/core/tx/coordinator
    ydb/core/tx/mediator
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_allocator
    ydb/core/tx/tx_proxy

    ydb/library/actors/core
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
