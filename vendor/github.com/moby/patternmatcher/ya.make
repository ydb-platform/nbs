GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    patternmatcher.go
)

GO_TEST_SRCS(patternmatcher_test.go)

END()

RECURSE(
    gotest
    ignorefile
)
