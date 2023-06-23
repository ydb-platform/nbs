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
    library/cpp/actors/core
    library/cpp/lwtrace
    ydb/core/base
    ydb/core/protos
)

END()
