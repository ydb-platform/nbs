GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    decode.go
    doc.go
    encode.go
    schema.go
    types.go
)

GO_TEST_SRCS(
    decode_test.go
    schema_test.go
)

GO_XTEST_SRCS(encode_test.go)

END()

RECURSE(
    gotest
)
