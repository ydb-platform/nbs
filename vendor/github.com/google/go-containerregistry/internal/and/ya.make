GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    and_closer.go
)

GO_TEST_SRCS(and_closer_test.go)

END()

RECURSE(
    gotest
)
