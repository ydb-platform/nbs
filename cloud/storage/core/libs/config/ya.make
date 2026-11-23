LIBRARY()

SRCS(
    config_schema_path.cpp
    runtime_config.cpp
)

PEERDIR(
    cloud/storage/core/config
)

END()

RECURSE_FOR_TESTS(
    ut
)
