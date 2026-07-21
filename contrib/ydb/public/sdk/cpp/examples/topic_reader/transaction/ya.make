PROGRAM(read_from_topic_in_transaction)

SRCS(
    application.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/query
    library/cpp/getopt
)

END()
