PROGRAM(config_proto_plugin)

PEERDIR(
    contrib/libs/protoc
    contrib/ydb/public/lib/protobuf
    contrib/ydb/core/config/protos
)

SRCS(
    main.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
