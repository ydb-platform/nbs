GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ecies_aead_hkdf_dem_helper.go
    ecies_aead_hkdf_private_key_manager.go
    ecies_aead_hkdf_public_key_manager.go
    hpke_private_key_manager.go
    hpke_public_key_manager.go
    hybrid.go
    hybrid_decrypt_factory.go
    hybrid_encrypt_factory.go
    hybrid_key_templates.go
)

GO_TEST_SRCS(
    ecies_aead_hkdf_dem_helper_test.go
    ecies_aead_hkdf_hybrid_decrypt_test.go
    ecies_aead_hkdf_hybrid_encrypt_test.go
    hpke_private_key_manager_test.go
    hpke_public_key_manager_test.go
)

GO_XTEST_SRCS(
    hybrid_factory_test.go
    hybrid_key_templates_test.go
    hybrid_test.go
)

END()

RECURSE(
    gotest
    internal
    subtle
)
