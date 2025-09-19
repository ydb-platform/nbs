DLL(filestore-fsdev)
EXPORTS_SCRIPT(fsdev.symlist)

SRCS(
    fsdev.cpp
    vfsdev_passthru.c
    vfsdev_passthru_rpc.c
)

ADDINCL(
    cloud/filestore/apps/fsdev/spdk/include
)

PEERDIR(
)

END()
