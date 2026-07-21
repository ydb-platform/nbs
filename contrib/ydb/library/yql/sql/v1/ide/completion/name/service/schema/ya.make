LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/object
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
    library/cpp/iterator
)

END()
