G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    storage_group_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/storage_group

    cloud/fastshard/bootstrap
    cloud/fastshard/sn/iface
    cloud/fastshard/testlib

    cloud/storage/core/libs/common

    contrib/libs/silk/src/fibers
    contrib/restricted/google/benchmark

    library/cpp/getopt
)

END()
