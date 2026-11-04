UNITTEST_FOR(cloud/storage/core/libs/kikimr/monitoring)

SRCS(
    monitoring_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics
    library/cpp/http/simple
    library/cpp/json
    library/cpp/testing/common
)

DATA(
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.crt
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.key
)

END()
