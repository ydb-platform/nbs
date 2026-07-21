LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/core
    contrib/ydb/library/yql/sql/v1/ide/completion/name/service
)

END()

RECURSE(
    cache
    cluster
    object
    service
)
