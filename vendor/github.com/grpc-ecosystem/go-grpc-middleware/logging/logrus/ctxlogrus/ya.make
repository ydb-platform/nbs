GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    context.go
    doc.go
    noop.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    # gotest
)
