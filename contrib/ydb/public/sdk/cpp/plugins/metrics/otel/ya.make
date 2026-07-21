LIBRARY()

SRCS(
    metrics.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/metrics
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/libs/opentelemetry-cpp
    contrib/libs/opentelemetry-cpp/api
)

END()
