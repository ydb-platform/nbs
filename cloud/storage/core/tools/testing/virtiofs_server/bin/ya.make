PROGRAM(virtiofs-server)

SRCS(main.cpp)

PEERDIR(
    cloud/filestore/libs/client
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/service
    cloud/filestore/libs/service_local
    cloud/filestore/libs/vfs
    cloud/filestore/libs/vfs_fuse/vhost
    cloud/filestore/libs/vhost
    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/file_backed_containers
    library/cpp/getopt
    library/cpp/monlib/dynamic_counters
)

END()
