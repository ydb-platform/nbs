PROGRAM(nbs-request-timing-workload)

SRCS(main.cpp)

PEERDIR(
    cloud/blockstore/libs/service
    cloud/blockstore/libs/diagnostics
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    library/cpp/monlib/dynamic_counters
)

END()
