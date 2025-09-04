GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    options.go
    scheme.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
