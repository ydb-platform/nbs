GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dump.go
    pointermap.go
    utils.go
)

GO_TEST_SRCS(dump_test.go)

END()

RECURSE(
    gotest
)
