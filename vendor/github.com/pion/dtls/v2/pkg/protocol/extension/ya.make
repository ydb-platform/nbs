GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    alpn.go
    errors.go
    extension.go
    renegotiation_info.go
    server_name.go
    srtp_protection_profile.go
    supported_elliptic_curves.go
    supported_point_formats.go
    supported_signature_algorithms.go
    use_master_secret.go
    use_srtp.go
)

GO_TEST_SRCS(
    alpn_test.go
    extension_test.go
    renegotiation_info_test.go
    server_name_test.go
    supported_elliptic_curves_test.go
    supported_point_formats_test.go
    supported_signature_algorithms_test.go
    use_srtp_test.go
)

END()

RECURSE(
    gotest
)
