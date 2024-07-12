GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    buffer.go
    errors.go
    no_hardlimit.go
)

GO_TEST_SRCS(buffer_test.go)

END()

RECURSE(
    gotest
)
