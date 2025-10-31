LIBRARY()

SRCS(
    bootstrap.cpp
    config_initializer.cpp
    options.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/client
    cloud/filestore/libs/daemon/common
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/endpoint
    cloud/filestore/libs/endpoint_vhost
    cloud/filestore/libs/server
    cloud/filestore/libs/service
    cloud/filestore/libs/service_kikimr
    cloud/filestore/libs/service_local
    cloud/filestore/libs/service_null
    cloud/filestore/libs/vfs_fuse/vhost
    cloud/filestore/libs/vhost

    cloud/storage/core/libs/common
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/endpoints/fs
    cloud/storage/core/libs/endpoints/keyring
    cloud/storage/core/libs/io_uring
    cloud/storage/core/libs/kikimr

    ydb/core/blobstorage/lwtrace_probes
    ydb/core/tablet_flat

    library/cpp/lwtrace
    library/cpp/lwtrace/mon
)

END()

RECURSE_FOR_TESTS(
    ut
)
