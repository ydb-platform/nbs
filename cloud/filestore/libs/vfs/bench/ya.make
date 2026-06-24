Y_BENCHMARK()

SIZE(medium)
TIMEOUT(300)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    fsync_queue_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/vfs
    library/cpp/histogram/hdr
)

END()
