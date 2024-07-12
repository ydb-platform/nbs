GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ipfamily.go
    ipnet.go
    net.go
    parse.go
    port.go
)

GO_TEST_SRCS(
    ipfamily_test.go
    ipnet_test.go
    net_test.go
    port_test.go
)

END()

RECURSE(
    gotest
)
