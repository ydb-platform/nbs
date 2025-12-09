PROGRAM()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    main.cpp
    ttl.h
    ttl.cpp
    util.h
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
