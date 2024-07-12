GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    insecurecleartextkeyset.go
)

GO_XTEST_SRCS(insecurecleartextkeyset_test.go)

END()

RECURSE(
    gotest
)
