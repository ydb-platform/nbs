PROGRAM(checksum-bench)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/protos
    library/cpp/digest/crc32c
)

END()
