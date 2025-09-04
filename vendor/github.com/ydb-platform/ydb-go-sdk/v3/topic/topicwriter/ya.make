GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    topicwriter.go
    write_options.go
)

GO_XTEST_SRCS(topicwriter_test.go)

END()

RECURSE(
    gotest
)
