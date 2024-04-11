LIBRARY()

SRCS(
    features_config.cpp
    filters.cpp
)

PEERDIR(
    cloud/storage/core/config
)

END()

RECURSE_FOR_TESTS(
    ut
)
