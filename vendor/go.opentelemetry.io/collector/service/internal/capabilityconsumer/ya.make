GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    capabilities.go
)

GO_TEST_SRCS(capabilities_test.go)

END()

RECURSE(
    gotest
)
