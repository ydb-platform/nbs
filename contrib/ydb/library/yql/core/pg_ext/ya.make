LIBRARY()

SRCS(
    yql_pg_ext.cpp
)

PEERDIR(
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/parser/pg_catalog
)

END()
