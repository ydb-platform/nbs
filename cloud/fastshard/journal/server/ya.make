LIBRARY()

SRCS(
    builder.cpp
    config.cpp
    device_manager.cpp
    request.cpp
    server.cpp
    service.cpp
)

PEERDIR(
    cloud/fastshard/journal/iface
    cloud/fastshard/journal/impl
    cloud/fastshard/protos
    cloud/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/coroutine/listener
)

END()

RECURSE_FOR_TESTS(
    ut
)
