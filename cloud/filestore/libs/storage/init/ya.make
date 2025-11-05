LIBRARY()

SRCS(
    actorsystem.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/service
    cloud/filestore/libs/storage/ss_proxy
    cloud/filestore/libs/storage/tablet
    cloud/filestore/libs/storage/tablet_proxy

    cloud/storage/core/libs/api
    cloud/storage/core/libs/auth
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/hive_proxy
    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/user_stats
    cloud/storage/core/libs/version_ydb

    contrib/ydb/library/actors/core

    contrib/ydb/core/base
    contrib/ydb/core/driver_lib/run
    contrib/ydb/core/mind
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/public/lib/deprecated/kicli
)

YQL_LAST_ABI_VERSION()

END()
