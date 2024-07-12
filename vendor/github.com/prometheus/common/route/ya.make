GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    route.go
)

GO_TEST_SRCS(route_test.go)

END()

RECURSE(
    gotest
)
