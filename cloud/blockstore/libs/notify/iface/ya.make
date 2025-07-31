LIBRARY()

SRCS(
    notify.cpp
    config.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
