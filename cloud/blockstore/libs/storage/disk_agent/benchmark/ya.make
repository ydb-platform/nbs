G_BENCHMARK()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ../rdma_target_benchmark.cpp
    ../recent_blocks_tracker_benchmark.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/storage/disk_agent
    cloud/blockstore/libs/storage/testlib
)

END()
