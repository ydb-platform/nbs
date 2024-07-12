GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auto.go
)

GO_TEST_SRCS(auto_test.go)

END()

RECURSE(
    gotest
)
