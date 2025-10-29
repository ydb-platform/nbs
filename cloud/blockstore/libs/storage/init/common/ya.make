LIBRARY()

SRCS(
    actorsystem.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api

    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/version_ydb

    ydb/library/actors/core
    ydb/library/actors/util
    library/cpp/logger
    library/cpp/monlib/service/pages

    ydb/core/driver_lib/run
)

YQL_LAST_ABI_VERSION()

END()
