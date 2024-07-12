GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    cbor.go
    coerce.go
    const.go
    decode.go
    encode.go
    float16.go
)

GO_TEST_SRCS(
    coerce_test.go
    decode_test.go
    encode_test.go
    float16_test.go
)

END()

RECURSE(
    gotest
)
