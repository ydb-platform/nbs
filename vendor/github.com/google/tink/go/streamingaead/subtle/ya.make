GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aes_ctr_hmac.go
    aes_gcm_hkdf.go
    subtle.go
)

GO_XTEST_SRCS(
    aes_ctr_hmac_test.go
    aes_gcm_hkdf_test.go
    subtle_test.go
)

END()

RECURSE(
    gotest
    noncebased
)
