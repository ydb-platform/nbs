GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    context.go
    crypto.go
    errors.go
    key_derivation.go
    keying.go
    option.go
    protection_profile.go
    session.go
    session_srtcp.go
    session_srtp.go
    srtcp.go
    srtp.go
    srtp_cipher.go
    srtp_cipher_aead_aes_gcm.go
    srtp_cipher_aes_cm_hmac_sha1.go
    stream.go
    stream_srtcp.go
    stream_srtp.go
    util.go
)

GO_TEST_SRCS(
    context_test.go
    crypto_test.go
    key_derivation_test.go
    keying_test.go
    protection_profile_test.go
    session_srtcp_test.go
    session_srtp_test.go
    srtcp_test.go
    srtp_cipher_aead_aes_gcm_test.go
    srtp_test.go
    stream_srtp_test.go
)

END()

RECURSE(
    gotest
)
