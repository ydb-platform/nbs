LIBRARY()

SRCS(
    app.cpp
    device_manager.cpp
    options.cpp
)

PEERDIR(
    cloud/fastshard/libs/journal/iface
    cloud/fastshard/libs/journal/impl
    cloud/fastshard/libs/journal/server
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/getopt
    library/cpp/string_utils/parse_size
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
