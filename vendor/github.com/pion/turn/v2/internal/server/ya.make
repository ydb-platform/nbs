GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    errors.go
    server.go
    stun.go
    turn.go
    util.go
)

GO_TEST_SRCS(turn_test.go)

END()

RECURSE(
    gotest
)
