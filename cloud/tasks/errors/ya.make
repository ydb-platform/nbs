GO_LIBRARY()

SET(
    GO_VET_FLAGS
    -printf=false
)

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
