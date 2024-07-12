GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    client.go
    config.go
    coordinate.go
    phantom.go
)

GO_TEST_SRCS(
    client_test.go
    coordinate_test.go
    performance_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
