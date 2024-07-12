GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    gzkp.go
)

GO_TEST_SRCS(gzkp_test.go)

END()

RECURSE(
    gotest
)
