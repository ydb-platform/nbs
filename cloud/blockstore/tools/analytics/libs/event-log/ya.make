LIBRARY()

SRCS(
    dump.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/storage/core/libs/common
    library/cpp/json
)

END()
