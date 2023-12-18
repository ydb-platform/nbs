GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    connections_state.go
    ctx.go
    local_dc.go
)

GO_TEST_SRCS(
    balancer_test.go
    connections_state_test.go
    local_dc_test.go
)

END()

RECURSE(
    config
    gotest
)
