PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
