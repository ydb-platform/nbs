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

    contrib/libs/opentelemetry-proto

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
