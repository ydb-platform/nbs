LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service/ranking
)

END()
