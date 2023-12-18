GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    topicwriter.go
)

GO_XTEST_SRCS(topicwriter_test.go)

END()

RECURSE(
    gotest
)
