GO_LIBRARY()

SRCS(
    lister.go
    filestore_lister.go
)

GO_TEST_SRCS(
    filestore_lister_test.go
)

END()

RECURSE(
    mocks
)

RECURSE_FOR_TESTS(
    tests
)
