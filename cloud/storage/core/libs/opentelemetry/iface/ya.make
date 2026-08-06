LIBRARY()

SRCS(
    trace_service_client.cpp
)

PEERDIR(
    cloud/blockstore/config

    contrib/proto/opentelemetry

    library/cpp/threading/future
)

END()
