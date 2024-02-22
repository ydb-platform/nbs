LIBRARY()

SRCS(
    features_config.cpp
)

PEERDIR(
    cloud/storage/core/config
)

END()

RECURSE_FOR_TESTS(
    ut
)
