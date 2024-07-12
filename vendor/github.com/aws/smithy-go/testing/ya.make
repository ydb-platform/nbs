GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bytes.go
    cbor.go
    doc.go
    document.go
    errors.go
    gzip.go
    reader.go
    rest.go
    struct.go
)

GO_TEST_SRCS(
    document_test.go
    struct_test.go
)

END()

RECURSE(
    gotest
    xml
)
