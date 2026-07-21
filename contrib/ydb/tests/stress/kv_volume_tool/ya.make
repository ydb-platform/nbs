PROGRAM(kv_volume_tool)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/grpc
    contrib/libs/protobuf
    library/cpp/getopt
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    tests
)
