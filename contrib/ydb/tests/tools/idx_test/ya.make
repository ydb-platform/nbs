PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/resource
    contrib/ydb/public/lib/idx_test
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
)

RESOURCE(
    ./sql/create_table1.sql create_table1
)

END()
