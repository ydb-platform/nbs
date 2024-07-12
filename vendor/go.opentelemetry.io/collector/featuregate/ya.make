GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    flag.go
    gate.go
    registry.go
    stage.go
)

GO_TEST_SRCS(
    flag_test.go
    gate_test.go
    registry_test.go
    stage_test.go
)

END()

RECURSE(
    gotest
)
