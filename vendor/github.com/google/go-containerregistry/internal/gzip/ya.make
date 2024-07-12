GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    zip.go
)

GO_TEST_SRCS(zip_test.go)

END()

RECURSE(
    gotest
)
