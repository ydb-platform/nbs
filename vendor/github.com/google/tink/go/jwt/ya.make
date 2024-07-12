GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    jwk_converter.go
    jwt.go
    jwt_ecdsa_signer_key_manager.go
    jwt_ecdsa_verifier_key_manager.go
    jwt_encoding.go
    jwt_hmac_key_manager.go
    jwt_key_templates.go
    jwt_mac.go
    jwt_mac_factory.go
    jwt_mac_kid.go
    jwt_signer.go
    jwt_signer_factory.go
    jwt_signer_kid.go
    jwt_validator.go
    jwt_verifier.go
    jwt_verifier_factory.go
    jwt_verifier_kid.go
    raw_jwt.go
    verified_jwt.go
)

GO_TEST_SRCS(
    jwt_ecdsa_signer_key_manager_test.go
    jwt_ecdsa_verifier_key_manager_test.go
    jwt_encoding_test.go
    jwt_hmac_key_manager_test.go
    jwt_mac_kid_test.go
    jwt_signer_verifier_kid_test.go
)

GO_XTEST_SRCS(
    jwk_converter_test.go
    jwt_key_templates_test.go
    jwt_mac_factory_test.go
    jwt_signer_verifier_factory_test.go
    jwt_test.go
    jwt_validator_test.go
    raw_jwt_test.go
    verified_jwt_test.go
)

END()

RECURSE(
    gotest
)
