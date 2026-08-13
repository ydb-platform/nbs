PROGRAM(nbs-rdma-rnr-repro)

BUILD_ONLY_IF(WARNING OS_LINUX)

SRCS(
    main.c
)

PEERDIR(
    contrib/libs/ibdrv
)

END()
