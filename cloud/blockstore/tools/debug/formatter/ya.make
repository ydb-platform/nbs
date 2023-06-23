PROGRAM(blockstore-debug-formatter)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/common

    library/cpp/getopt
)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/apps/common/restrict.inc)

END()
