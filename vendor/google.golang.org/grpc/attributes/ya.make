GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attributes.go
)

GO_XTEST_SRCS(attributes_test.go)

END()

RECURSE(
    gotest
)
