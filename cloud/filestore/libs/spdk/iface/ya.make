LIBRARY()

SRCS(
    config.cpp
    env.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
)

END()
