GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    consumed_units.go
    context.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
