LIBRARY()

SRCS(
    trace_service_client.cpp
)

PEERDIR(
    cloud/blockstore/config

    contrib/libs/opentelemetry-proto

    library/cpp/threading/future
)

END()
