GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    context_keys.go
    doc.go
    downstream_request.go
)

GO_XTEST_SRCS(downstream_request_test.go)

END()

RECURSE(
    gotest
)
