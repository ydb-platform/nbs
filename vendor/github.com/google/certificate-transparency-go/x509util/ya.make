GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    files.go
    fuzz.go
    pem_cert_pool.go
    revoked.go
    x509util.go
)

GO_XTEST_SRCS(
    certs_test.go
    pem_cert_pool_test.go
)

END()

RECURSE(
    gotest
)
