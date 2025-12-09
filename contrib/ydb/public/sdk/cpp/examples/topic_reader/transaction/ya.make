PROGRAM(read_from_topic_in_transaction)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    application.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic
    library/cpp/getopt
)

END()
