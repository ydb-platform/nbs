GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bucket.go
    conn.go
    doc.go
    handler.go
    listener.go
    utils.go
)

GO_TEST_SRCS(
    bucket_test.go
    handler_test.go
    listener_test.go
)

END()

RECURSE(
    gotest
)
