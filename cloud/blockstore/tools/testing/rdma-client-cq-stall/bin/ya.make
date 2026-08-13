PROGRAM(nbs-rdma-client-cq-stall-test)

BUILD_ONLY_IF(WARNING OS_LINUX)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/rdma
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/service
    cloud/blockstore/libs/service_local
    cloud/blockstore/libs/storage/disk_agent
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface
    library/cpp/monlib/dynamic_counters
    library/cpp/testing/unittest
)

END()
