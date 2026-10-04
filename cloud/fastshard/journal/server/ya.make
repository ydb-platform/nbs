LIBRARY()

SRCS(
    server.cpp
)

PEERDIR(
    cloud/fastshard/journal/iface
    cloud/fastshard/protos

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
