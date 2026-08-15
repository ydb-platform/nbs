G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    ../client_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/client
    cloud/filestore/libs/storage/fastshard/sn/iface
    cloud/filestore/libs/storage/fastshard/sn/server
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers
)

END()
