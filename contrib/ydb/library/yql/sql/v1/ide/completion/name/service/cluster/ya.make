LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cluster
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
    library/cpp/case_insensitive_string
)

END()
