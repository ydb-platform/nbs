GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    btree_generic.go
)

GO_TEST_SRCS(
    btree_generic_test.go
    btree_test.go
)

END()

RECURSE(
    # gotest
)
