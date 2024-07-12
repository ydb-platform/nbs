GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    logging.go
    logging_http_transport.go
    transport.go
)

GO_XTEST_SRCS(
    # logging_http_transport_test.go
)

END()

RECURSE(
    gotest
)
