GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    iptables.go
    lock.go
)

GO_TEST_SRCS(iptables_test.go)

END()

RECURSE(
    # gotest
)
