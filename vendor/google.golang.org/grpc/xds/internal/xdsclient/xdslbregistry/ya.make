GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    xdslbregistry.go
)

GO_XTEST_SRCS(xdslbregistry_test.go)

END()

RECURSE(
    converter
    gotest
)
