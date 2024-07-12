GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aes_ctr_hmac_key_manager.go
    aes_gcm_hkdf_key_manager.go
    decrypt_reader.go
    streamingaead.go
    streamingaead_factory.go
    streamingaead_key_templates.go
)

GO_TEST_SRCS(decrypt_reader_test.go)

GO_XTEST_SRCS(
    aes_ctr_hmac_key_manager_test.go
    aes_gcm_hkdf_key_manager_test.go
    streamingaead_factory_test.go
    streamingaead_key_templates_test.go
    streamingaead_test.go
)

END()

RECURSE(
    gotest
    subtle
)
