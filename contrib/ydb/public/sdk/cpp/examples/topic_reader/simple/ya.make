PROGRAM(simple_persqueue_reader)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic
    library/cpp/getopt
)

END()
