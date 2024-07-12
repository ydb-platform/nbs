GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    mitm.go
)

GO_TEST_SRCS(mitm_test.go)

END()

RECURSE(
    gotest
)
