GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    net.go
)

GO_TEST_SRCS(net_test.go)

END()

RECURSE(
    gotest
)
