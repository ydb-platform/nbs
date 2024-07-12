GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    extension.go
)

GO_TEST_SRCS(extension_test.go)

END()

RECURSE(
    experimental
    extensiontest
    gotest
)
