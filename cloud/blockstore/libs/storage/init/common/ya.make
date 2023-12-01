LIBRARY()

SRCS(
    actorsystem.cpp
)

PEERDIR(
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api

    cloud/storage/core/libs/kikimr

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    library/cpp/logger
    library/cpp/monlib/service/pages

    contrib/ydb/core/driver_lib/run
)

YQL_LAST_ABI_VERSION()

END()
