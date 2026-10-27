G_BENCHMARK()

TAG(ya:manual)

SRCS(
    storage_group_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/storage_group

    cloud/fastshard/libs/bootstrap
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/testlib

    cloud/storage/core/libs/common

    contrib/libs/silk/src/fibers
    contrib/restricted/google/benchmark

    library/cpp/getopt
)

END()
