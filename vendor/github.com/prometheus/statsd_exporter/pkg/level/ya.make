GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    level.go
)

GO_TEST_SRCS(level_test.go)

END()

RECURSE(
    gotest
)
