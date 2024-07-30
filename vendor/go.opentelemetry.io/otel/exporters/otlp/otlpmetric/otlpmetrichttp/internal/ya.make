GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    gen.go
    partialsuccess.go
)

GO_TEST_SRCS(partialsuccess_test.go)

END()

RECURSE(
    envconfig
    gotest
    oconf
    otest
    retry
    transform
)
