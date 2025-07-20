Y_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    mixed_blocks_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/tablet/model
)

END()
