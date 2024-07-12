GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    interceptors.go
    options.go
)

GO_XTEST_SRCS(
    examples_test.go
    interceptors_test.go
)

END()

RECURSE(
    # gotest
)
