DLL(filestore-fsdev)
EXPORTS_SCRIPT(fsdev.symlist)

SRCS(
    fsdev.c
)

PEERDIR(
    cloud/filestore/libs/spdk
)

END()
