GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.4.0)

SRCS(
    backoff.go
    doc.go
    options.go
    retry.go
)

GO_XTEST_SRCS(
    examples_test.go
    retry_test.go
)

END()

RECURSE(
    # gotest
)
