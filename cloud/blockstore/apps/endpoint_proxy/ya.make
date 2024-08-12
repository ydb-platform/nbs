PROGRAM(blockstore-endpoint-proxy)

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/endpoint_proxy/server
    cloud/storage/core/libs/daemon

    library/cpp/getopt

    contrib/libs/grpc/grpc++_reflection
)

END()
