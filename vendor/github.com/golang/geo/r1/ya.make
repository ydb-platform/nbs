GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    interval.go
)

GO_TEST_SRCS(interval_test.go)

END()

RECURSE(
    gotest
)
