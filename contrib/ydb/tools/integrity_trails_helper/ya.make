PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/core/engine
    contrib/ydb/core/scheme
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
