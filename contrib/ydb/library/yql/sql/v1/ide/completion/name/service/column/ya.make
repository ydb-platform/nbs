LIBRARY()

SRCS(
    name_service.cpp
    replicating.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/analysis/global
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object/simple/static
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
)

END()
