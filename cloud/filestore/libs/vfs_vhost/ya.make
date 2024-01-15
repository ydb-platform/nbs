LIBRARY()

OWNER(g:cloud-nbs)

SRCS(
    context.cpp
    fs.cpp
    fs_impl.cpp
    fs_impl_init.cpp
    loop.cpp
    vfs.cpp
    vfs_vhost.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/client
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/service
    cloud/filestore/libs/vfs
    cloud/filestore/libs/vfs/protos
    cloud/filestore/libs/vhost

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    cloud/contrib/vhost

    library/cpp/logger

    contrib/libs/linux-headers
)

END()

RECURSE_FOR_TESTS(
    ut
)
