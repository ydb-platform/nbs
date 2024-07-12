GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    providerserver.go
    serve_opts.go
)

GO_TEST_SRCS(
    providerserver_test.go
    serve_opts_test.go
)

END()

RECURSE(
    gotest
)
