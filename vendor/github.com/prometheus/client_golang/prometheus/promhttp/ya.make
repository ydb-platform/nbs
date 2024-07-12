GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    delegator.go
    http.go
    instrument_client.go
    instrument_server.go
    option.go
)

GO_TEST_SRCS(
    http_test.go
    instrument_client_test.go
    instrument_server_test.go
    option_test.go
)

END()

RECURSE(
    # gotest
)
