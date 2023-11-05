PROGRAM(table-perf)

SRCS(
    colons.cpp
    main.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat/test/libs/table
    library/cpp/charset
    library/cpp/getopt
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
