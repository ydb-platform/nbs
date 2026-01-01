Y_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    tablet_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

PEERDIR(
    cloud/filestore/libs/storage/tablet
)

YQL_LAST_ABI_VERSION()

END()
