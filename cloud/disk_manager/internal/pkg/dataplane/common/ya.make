GO_LIBRARY()

SRCS(
    chunk.go
    transfer.go
)

GO_TEST_SRCS(
    transfer_test.go
    chunk_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
