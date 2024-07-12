GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    hclsimple.go
)

GO_XTEST_SRCS(hclsimple_test.go)

END()

RECURSE(
    gotest
)
