GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    boundaries.go
    bucket.go
    spanprocessor.go
    templates.go
    tracez.go
    version.go
)

GO_TEST_SRCS(
    boundaries_test.go
    bucket_test.go
    spanprocessor_test.go
)

END()

RECURSE(
    gotest
    internal
)
