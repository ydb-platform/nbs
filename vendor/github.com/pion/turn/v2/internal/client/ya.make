GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    allocation.go
    binding.go
    client.go
    errors.go
    periodic_timer.go
    permission.go
    tcp_alloc.go
    tcp_conn.go
    transaction.go
    trylock.go
    udp_conn.go
)

GO_TEST_SRCS(
    binding_test.go
    client_test.go
    periodic_timer_test.go
    permission_test.go
    tcp_conn_test.go
    trylock_test.go
    udp_conn_test.go
)

END()

RECURSE(
    gotest
)
