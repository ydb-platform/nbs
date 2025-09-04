GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    batch_options.go
    doc.go
    errors.go
    reader.go
)

GO_XTEST_SRCS(reader_example_test.go)

END()

RECURSE(
    gotest
)
