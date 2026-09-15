LIBRARY()

SRCS(
    configs_manager.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/config/init
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yaml_config

    library/cpp/monlib/dynamic_counters
)

END()

RECURSE_FOR_TESTS(
    ut
)
