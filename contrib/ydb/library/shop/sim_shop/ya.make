PROGRAM()

RESOURCE(
    one.pb.txt one
    two.pb.txt two
)

SRCS(
    myshopmain.cpp
    myshop.cpp
    config.proto
)

PEERDIR(
    contrib/ydb/library/drr
    library/cpp/lwtrace/mon
    contrib/ydb/library/shop
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/resource
    library/cpp/protobuf/util
)

END()
