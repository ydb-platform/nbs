GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    parsepath.go
    parseutil.go
)

GO_TEST_SRCS(
    parsepath_test.go
    parseutil_test.go
)

END()

RECURSE(
    gotest
)
