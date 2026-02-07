PROGRAM(pg_catalog_dump)

SRCS(
    pg_catalog_dump.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/utils/backtrace
    library/cpp/json
)

END()
