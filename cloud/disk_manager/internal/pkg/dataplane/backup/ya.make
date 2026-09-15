GO_LIBRARY()

SRCS(
    meta.go
    slaves.go
)

GO_TEST_SRCS(
    meta_test.go
)

END()

RECURSE(
    layout
)

RECURSE_FOR_TESTS(
    tests
)
