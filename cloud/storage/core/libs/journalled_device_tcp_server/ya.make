LIBRARY()

SRCS(
    server.cpp
)

PEERDIR(
    cloud/storage/core/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/journalled_device

    library/cpp/coroutine/listener
)

END()

RECURSE_FOR_TESTS(
    ut
)
