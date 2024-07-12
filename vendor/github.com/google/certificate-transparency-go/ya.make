GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    proto_gen.go
    serialization.go
    signatures.go
    types.go
)

GO_TEST_SRCS(
    serialization_test.go
    signatures_test.go
    types_test.go
)

END()

RECURSE(
    asn1
    gossip
    gotest
    testdata
    tls
    x509
    x509util
)
