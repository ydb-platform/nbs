OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    compressor.go
)

GO_TEST_SRCS(
    compressor_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
