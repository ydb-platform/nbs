PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/sdk/cpp/src/client/iam_private
)

END()
