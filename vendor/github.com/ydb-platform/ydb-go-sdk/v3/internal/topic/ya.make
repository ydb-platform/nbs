GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    configs.go
    convertors.go
    retriable_error.go
)

GO_TEST_SRCS(retriable_error_test.go)

END()

RECURSE(
    gotest
    topicclientinternal
    topiclistenerinternal
    topicreadercommon
    topicreaderinternal
    topicwriterinternal
)
