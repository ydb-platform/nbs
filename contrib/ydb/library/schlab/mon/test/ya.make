PROGRAM(mon-test)

RESOURCE(
    ../static/schviz-test0.json schlab/schviz-test0.json
)

SRCS(
    test.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/schlab/mon
)

END()
