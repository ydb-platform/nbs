GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    error.go
    transform.go
    transformer.go
)

GO_TEST_SRCS(transform_test.go)

END()

RECURSE(
    gotest
)
