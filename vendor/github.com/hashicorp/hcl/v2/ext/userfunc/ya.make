GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    decode.go
    doc.go
    public.go
)

GO_TEST_SRCS(decode_test.go)

END()

RECURSE(
    gotest
)
