LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    contrib/ydb/library/yql/public/issue
)

END()
