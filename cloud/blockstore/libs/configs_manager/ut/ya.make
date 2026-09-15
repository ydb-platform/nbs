UNITTEST_FOR(cloud/blockstore/libs/configs_manager)

SRCS(
    configs_manager_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/storage/core

    contrib/ydb/core/cms/console
    contrib/ydb/core/protos
    contrib/ydb/core/testlib
)

END()
