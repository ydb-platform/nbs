LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
    client_pid.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    yql/essentials/public/issue
)

END()
