G_BENCHMARK()

SRCS(
    ../recent_blocks_tracker_benchmark.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/testlib
)

END()
