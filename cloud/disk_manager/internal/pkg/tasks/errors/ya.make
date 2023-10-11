OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    errors.go
)

GO_TEST_SRCS(
    errors_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
