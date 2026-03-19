Y_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    session_bench.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/client
    cloud/filestore/libs/service
    cloud/filestore/public/api/grpc
    cloud/filestore/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
