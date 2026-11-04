LIBRARY()

SRCS(
    bootstrap_metrics.h
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    library/cpp/http/simple
    library/cpp/json
    library/cpp/testing/common
    library/cpp/testing/unittest
)

END()
