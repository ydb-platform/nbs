PROGRAM(blockstore-nvme)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/local_nvme

    library/cpp/getopt
)

SPLIT_DWARF()

END()
