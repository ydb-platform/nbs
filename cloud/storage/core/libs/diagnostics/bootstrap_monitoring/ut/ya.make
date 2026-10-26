UNITTEST_FOR(cloud/storage/core/libs/diagnostics/bootstrap_monitoring)

SRCS(
    bootstrap_monitoring_ut.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
)

DATA(
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.crt
    arcadia/cloud/storage/core/libs/grpc/ut/certs/server1.key
)

END()
