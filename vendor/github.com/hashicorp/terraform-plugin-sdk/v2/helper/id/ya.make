GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    id.go
)

GO_TEST_SRCS(id_test.go)

END()

RECURSE(
    gotest
)
