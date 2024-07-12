GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    chunk.go
    chunk_queue.go
    conn.go
    conn_map.go
    delay_filter.go
    errors.go
    loss_filter.go
    nat.go
    net.go
    resolver.go
    router.go
    tbf.go
    udpproxy.go
    udpproxy_direct.go
    vnet.go
)

GO_TEST_SRCS(
    chunk_queue_test.go
    chunk_test.go
    conn_map_test.go
    conn_test.go
    delay_filter_test.go
    loss_filter_test.go
    nat_test.go
    net_test.go
    resolver_test.go
    router_test.go
    stress_test.go
    tbf_test.go
    udpproxy_direct_test.go
    udpproxy_test.go
)

END()

RECURSE(
    gotest
)
