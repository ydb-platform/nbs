GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    balancer.go
    connections_state.go
    local_dc.go
)

GO_TEST_SRCS(
    connections_state_test.go
    local_dc_test.go
)

END()

RECURSE(
    cluster
    config
    gotest
)
