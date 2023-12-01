LIBRARY()

SRCS(
    components.cpp
    events.cpp
    helpers.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/protos
    cloud/storage/core/libs/kikimr
    contrib/ydb/library/actors/core
    library/cpp/lwtrace
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

END()
