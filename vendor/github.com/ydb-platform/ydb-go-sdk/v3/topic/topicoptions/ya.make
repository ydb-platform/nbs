GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    topicoptions.go
    topicoptions_alter.go
    topicoptions_create.go
    topicoptions_describe.go
    topicoptions_drop.go
    topicoptions_listener.go
    topicoptions_reader.go
    topicoptions_topic.go
    topicoptions_writer.go
)

GO_TEST_SRCS(topicoptions_test.go)

GO_XTEST_SRCS(
    # topicoptions_reader_example_test.go
    # topicoptions_writer_example_test.go
)

END()

RECURSE(
    gotest
)
