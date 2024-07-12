GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    cleanhttp.go
    doc.go
    handlers.go
)

GO_TEST_SRCS(handlers_test.go)

END()

RECURSE(
    gotest
)
