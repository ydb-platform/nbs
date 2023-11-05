LIBRARY()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/blockstore/core
    contrib/ydb/core/cms/console
    contrib/ydb/core/engine
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/filestore/core
    contrib/ydb/core/metering
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
    contrib/ydb/core/tx
    contrib/ydb/core/tx/datashard
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_allocator
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

SRCS(
    export_reboots_common.cpp
    failing_mtpq.cpp
    test_env.cpp
    test_env.h
    ls_checks.cpp
    ls_checks.h
    helpers.cpp
    helpers.h
)

YQL_LAST_ABI_VERSION()

END()
