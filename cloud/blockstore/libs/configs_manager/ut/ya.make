UNITTEST_FOR(cloud/blockstore/libs/configs_manager)

SRCS(
    configs_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/core
    cloud/storage/core/libs/diagnostics

    contrib/ydb/core/cms/console
    contrib/ydb/core/protos
    contrib/ydb/core/testlib

    library/cpp/logger
)

END()
