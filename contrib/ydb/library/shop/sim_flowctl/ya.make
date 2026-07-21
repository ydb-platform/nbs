PROGRAM()

SRCS(
    flowctlmain.cpp
)

PEERDIR(
    contrib/ydb/library/drr
    library/cpp/lwtrace/mon
    contrib/ydb/library/shop
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
)

END()
