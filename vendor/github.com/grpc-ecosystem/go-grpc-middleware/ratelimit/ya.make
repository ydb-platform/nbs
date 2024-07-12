GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    ratelimit.go
)

GO_TEST_SRCS(ratelimit_test.go)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    gotest
)
