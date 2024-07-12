GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    uuid.go
)

GO_TEST_SRCS(uuid_test.go)

END()

RECURSE(
    gotest
)
