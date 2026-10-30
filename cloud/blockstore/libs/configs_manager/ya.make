LIBRARY()

SRCS(
    configs_manager.cpp
    configs_manager_renderer.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/config
    cloud/storage/core/libs/diagnostics

    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core

    library/cpp/html/pcdata
)

END()

RECURSE_FOR_TESTS(
    ut
)
