GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    claims.go
    doc.go
    ecdsa.go
    ecdsa_utils.go
    ed25519.go
    ed25519_utils.go
    errors.go
    errors_go1_20.go
    hmac.go
    map_claims.go
    none.go
    parser.go
    parser_option.go
    registered_claims.go
    rsa.go
    rsa_pss.go
    rsa_utils.go
    signing_method.go
    token.go
    token_option.go
    types.go
    validator.go
)

GO_TEST_SRCS(
    # errors_test.go
    # map_claims_test.go
    # validator_test.go
)

GO_XTEST_SRCS(
    # ecdsa_test.go
    # ed25519_test.go
    # example_test.go
    # hmac_example_test.go
    # hmac_test.go
    # http_example_test.go
    # none_test.go
    # parser_test.go
    # rsa_pss_test.go
    # rsa_test.go
    # token_test.go
    # types_test.go
)

END()

RECURSE(
    cmd
    gotest
    request
    test
)
