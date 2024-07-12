GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestEnvVars)

SRCS(
    cert_pool.go
    curves.go
    error.go
    errors.go
    names.go
    pem_decrypt.go
    pkcs1.go
    pkcs8.go
    revoked.go
    root.go
    rpki.go
    sec1.go
    verify.go
    x509.go
)

GO_TEST_SRCS(
    curves_test.go
    errors_test.go
    name_constraints_test.go
    names_test.go
    pem_decrypt_test.go
    pkcs8_test.go
    revoked_test.go
    rpki_test.go
    sec1_test.go
    verify_test.go
    x509_test.go
)

GO_XTEST_SRCS(
    error_test.go
    example_test.go
)

IF (OS_LINUX)
    SRCS(
        root_linux.go
        root_unix.go
    )

    GO_TEST_SRCS(root_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        root_darwin.go
    )

    GO_TEST_SRCS(root_darwin_test.go)
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64 AND CGO_ENABLED)
    CGO_SRCS(root_cgo_darwin.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        root_darwin_armx.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        ptr_sysptr_windows.go
        root_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
    pkix
)
