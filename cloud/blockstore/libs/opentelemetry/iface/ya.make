LIBRARY()

SRCS(
    trace_service_client.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/iam/iface

    contrib/libs/opentelemetry-proto

    library/cpp/threading/future
)

END()
