GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auth.go
    doc.go
    metadata.go
)

GO_TEST_SRCS(metadata_test.go)

GO_XTEST_SRCS(
    auth_test.go
    examples_test.go
)

END()

RECURSE(
    # gotest
)
