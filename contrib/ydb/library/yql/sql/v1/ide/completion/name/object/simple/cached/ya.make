LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cache
)

END()
