LIBRARY()

SRCS(
    server.cpp
)

PEERDIR(
    cloud/storage/core/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics

    library/cpp/coroutine/listener
)

END()

RECURSE_FOR_TESTS(
    ut
)
