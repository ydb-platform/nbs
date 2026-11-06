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
    cloud/fastshard/libs/journal/iface
    cloud/fastshard/libs/journal/impl
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/coroutine/listener
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
