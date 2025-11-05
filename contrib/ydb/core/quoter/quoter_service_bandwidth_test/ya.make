PROGRAM()

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    contrib/ydb/core/base
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/quoter
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
    quota_requester.cpp
    server.cpp
)

END()
