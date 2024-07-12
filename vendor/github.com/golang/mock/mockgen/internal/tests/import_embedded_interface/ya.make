GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bugreport.go
    bugreport_mock.go
    foo.go
    net.go
    net_mock.go
)

GO_TEST_SRCS(
    bugreport_test.go
    net_test.go
)

END()

RECURSE(
    ersatz
    faux
    gotest
    other
)
