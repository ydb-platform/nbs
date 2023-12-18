GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    topicoptions
    topicreader
    topicsugar
    topictypes
    topicwriter
)
