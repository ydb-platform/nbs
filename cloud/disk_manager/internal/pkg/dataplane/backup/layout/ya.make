GO_LIBRARY()

SRCS(
    layout.go
)

GO_TEST_SRCS(
    layout_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
