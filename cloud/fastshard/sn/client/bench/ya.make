G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    ../client_bench.cpp
)

PEERDIR(
    cloud/fastshard/sn/client
    cloud/fastshard/sn/iface
    cloud/fastshard/sn/server
    cloud/fastshard/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers
)

END()
