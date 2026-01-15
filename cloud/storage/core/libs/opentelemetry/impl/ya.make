LIBRARY()

SRCS(
    helpers.cpp
    trace_convert.cpp
    trace_reader.cpp
    trace_service_client.cpp
)

PEERDIR(
    cloud/storage/core/config
    cloud/storage/core/libs/opentelemetry/iface
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/common

    contrib/libs/opentelemetry-proto
    contrib/libs/grpc

    library/cpp/threading/future
    library/cpp/lwtrace
)

END()

RECURSE_FOR_TESTS(ut)
