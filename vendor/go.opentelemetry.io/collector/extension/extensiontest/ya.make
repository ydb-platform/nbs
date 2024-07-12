GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    nop_extension.go
)

GO_TEST_SRCS(nop_extension_test.go)

END()

RECURSE(
    gotest
)
