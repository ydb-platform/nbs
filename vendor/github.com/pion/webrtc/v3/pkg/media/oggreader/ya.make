GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    oggreader.go
)

GO_TEST_SRCS(oggreader_test.go)

END()

RECURSE(
    gotest
)
