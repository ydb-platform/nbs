GO_LIBRARY()

SRCS(
    lister.go
    filestore_lister.go
)

END()

RECURSE(
    mocks
)

RECURSE_FOR_TESTS(
    tests
)
