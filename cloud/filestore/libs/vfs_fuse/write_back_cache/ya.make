LIBRARY()

SRCS(
    write_back_cache.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future/subscription
)

END()

RECURSE_FOR_TESTS(
    ut
)
