G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    ../client_bench.cpp
)

PEERDIR(
    cloud/fastshard/libs/bootstrap
    cloud/fastshard/libs/sn/client
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/libs/sn/server
    cloud/fastshard/libs/sn/testlib

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers
)

END()
