GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aead.go
    aes_gcm_aead.go
    chacha20poly1305_aead.go
    context.go
    decrypt.go
    encrypt.go
    hkdf_kdf.go
    hpke.go
    kdf.go
    kem.go
    primitive_factory.go
    x25519_kem.go
)

GO_TEST_SRCS(
    aes_gcm_aead_test.go
    chacha20poly1305_aead_test.go
    context_test.go
    encrypt_decrypt_test.go
    hkdf_kdf_test.go
    hpke_test.go
    primitive_factory_test.go
    x25519_kem_test.go
)

END()

RECURSE(
    gotest
)
