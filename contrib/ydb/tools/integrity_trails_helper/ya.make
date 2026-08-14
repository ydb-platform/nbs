PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/core/engine
    contrib/ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
