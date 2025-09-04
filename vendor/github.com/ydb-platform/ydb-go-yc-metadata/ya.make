GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.6.1)

SRCS(
    credentials.go
    error.go
    http.go
    metadata.go
    options.go
    pem.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    trace
)
