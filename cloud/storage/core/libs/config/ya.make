LIBRARY()

SRCS(
    runtime_config.cpp
)

PEERDIR(
    cloud/storage/core/config
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
