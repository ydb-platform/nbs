GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    confignet.go
    doc.go
)

GO_TEST_SRCS(confignet_test.go)

END()

RECURSE(
    gotest
)
