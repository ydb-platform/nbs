LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/backup/async_jobs
    contrib/ydb/core/tx/columnshard/backup/iscan
    contrib/ydb/core/tx/columnshard/backup/import
)

END()

RECURSE_FOR_TESTS(
    async_jobs
    import
    iscan
)
