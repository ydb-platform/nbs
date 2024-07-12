GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    info.go
    match_addrtype.go
    match_conntrack.go
    match_tcp.go
    match_udp.go
    target_dnat.go
    target_masquerade_ip.go
    unknown.go
    util.go
    xt.go
)

GO_TEST_SRCS(
    match_addrtype_test.go
    match_conntrack_test.go
    match_tcp_test.go
    match_udp_test.go
    target_dnat_test.go
    target_masquerade_ip_test.go
    unknown_test.go
)

END()

RECURSE(
    gotest
)
