GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

GO_SKIP_TESTS(
    TestScripts
    # calls go binary
)

SRCS(
    allhex.go
    proxy.go
    pseudo.go
)

GO_XTEST_SRCS(proxy_test.go)

END()

RECURSE(
    gotest
)
