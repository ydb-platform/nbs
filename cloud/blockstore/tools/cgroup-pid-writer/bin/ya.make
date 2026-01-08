PROGRAM(blockstore-cgroup-pid-writer)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/tools/cgroup-pid-writer/lib

    library/cpp/getopt
)

END()
