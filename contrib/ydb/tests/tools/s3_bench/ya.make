PROGRAM(ydb_s3_bench)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/wrappers
    contrib/ydb/core/wrappers/events
    contrib/ydb/core/util
    contrib/ydb/core/protos
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/getopt
    library/cpp/threading/future
)

END()


