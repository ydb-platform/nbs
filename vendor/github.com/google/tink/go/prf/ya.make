GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aes_cmac_prf_key_manager.go
    hkdf_prf_key_manager.go
    hmac_prf_key_manager.go
    prf_key_templates.go
    prf_set.go
    prf_set_factory.go
)

GO_XTEST_SRCS(
    aes_cmac_prf_key_manager_test.go
    hkdf_prf_key_manager_test.go
    hmac_prf_key_manager_test.go
    prf_key_templates_test.go
    prf_set_factory_test.go
    prf_test.go
)

END()

RECURSE(
    gotest
    subtle
)
