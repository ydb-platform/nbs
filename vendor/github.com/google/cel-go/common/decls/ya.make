GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    decls.go
)

GO_TEST_SRCS(decls_test.go)

END()

RECURSE(
    gotest
)
