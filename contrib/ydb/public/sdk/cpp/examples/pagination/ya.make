PROGRAM()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    main.cpp
    pagination_data.cpp
    pagination.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
