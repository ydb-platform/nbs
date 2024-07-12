GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    aes_128_ccm.go
    aes_256_ccm.go
    aes_ccm.go
    ciphersuite.go
    tls_ecdhe_ecdsa_with_aes_128_ccm.go
    tls_ecdhe_ecdsa_with_aes_128_ccm8.go
    tls_ecdhe_ecdsa_with_aes_128_gcm_sha256.go
    tls_ecdhe_ecdsa_with_aes_256_cbc_sha.go
    tls_ecdhe_ecdsa_with_aes_256_gcm_sha384.go
    tls_ecdhe_psk_with_aes_128_cbc_sha256.go
    tls_ecdhe_rsa_with_aes_128_gcm_sha256.go
    tls_ecdhe_rsa_with_aes_256_cbc_sha.go
    tls_ecdhe_rsa_with_aes_256_gcm_sha384.go
    tls_psk_with_aes_128_cbc_sha256.go
    tls_psk_with_aes_128_ccm.go
    tls_psk_with_aes_128_ccm8.go
    tls_psk_with_aes_128_gcm_sha256.go
    tls_psk_with_aes_256_ccm8.go
)

END()

RECURSE(
    types
)
