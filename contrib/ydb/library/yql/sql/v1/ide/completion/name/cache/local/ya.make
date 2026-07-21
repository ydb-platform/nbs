LIBRARY()

SRCS(
    cache.cpp
)

PEERDIR(
    contrib/ydb/library/yql/sql/v1/ide/completion/name/cache
    library/cpp/cache
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
