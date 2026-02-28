Y_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse
    cloud/filestore/libs/vfs_fuse/vhost
)

END()
