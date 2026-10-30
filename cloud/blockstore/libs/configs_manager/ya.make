LIBRARY()

SRCS(
    configs_manager.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/core/cms/console
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
