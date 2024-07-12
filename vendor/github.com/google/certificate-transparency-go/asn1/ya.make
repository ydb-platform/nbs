GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    asn1.go
    common.go
    marshal.go
)

GO_TEST_SRCS(
    asn1_test.go
    marshal_test.go
)

END()

RECURSE(
    gotest
)
