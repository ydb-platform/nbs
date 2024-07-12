GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    hashcode.go
)

GO_TEST_SRCS(hashcode_test.go)

END()

RECURSE(
    gotest
)
