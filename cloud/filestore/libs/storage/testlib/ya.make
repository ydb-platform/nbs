LIBRARY(filestore-libs-storage-testlib)

SRCS(
    helpers.cpp
    service_client.cpp
    ss_proxy_client.cpp
    tablet_client.cpp
    tablet_proxy_client.cpp
    test_env.cpp
    test_executor.cpp
    ut_helpers.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/model
    cloud/filestore/libs/storage/service
    cloud/filestore/libs/storage/ss_proxy
    cloud/filestore/libs/storage/tablet
    cloud/filestore/libs/storage/tablet_proxy
    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/hive_proxy
    cloud/storage/core/libs/kikimr
    ydb/library/actors/core
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/filestore/core
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/mind/hive
    ydb/core/security
    ydb/core/tablet_flat
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

END()
