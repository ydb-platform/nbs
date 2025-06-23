LIBRARY()

SRCS(
    helpers.cpp
    trace_convert.cpp
    trace_reader.cpp
    trace_service_client.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/opentelemetry/iface
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/iam/iface

    contrib/libs/opentelemetry-proto

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(ut)
