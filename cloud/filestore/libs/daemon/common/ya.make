LIBRARY(filestore-libs-daemon-common)

GENERATE_ENUM_SERIALIZATION(options.h)

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/diagnostics/metrics
    cloud/filestore/libs/server
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/init

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/features
    cloud/storage/core/libs/kikimr
    cloud/storage/core/libs/version

    library/cpp/lwtrace
)

END()
