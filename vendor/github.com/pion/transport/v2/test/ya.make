GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    bridge.go
    connctx.go
    rand.go
    stress.go
    test.go
    util.go
    util_nowasm.go
)

GO_TEST_SRCS(
    bridge_test.go
    test_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
