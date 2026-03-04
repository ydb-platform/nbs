GO_LIBRARY()

SRCS(
    storage.go
    storage_ydb.go
)

END()

RECURSE(
    mocks
)

RECURSE_FOR_TESTS(
    tests
)
