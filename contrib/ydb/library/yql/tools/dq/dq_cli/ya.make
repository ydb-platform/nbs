PROGRAM()

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/grpc/client
    library/cpp/protobuf/util
    library/cpp/threading/future
    contrib/ydb/library/yql/utils
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/yson_value
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/common
)

SRCS(
    main.cpp
)

END()
