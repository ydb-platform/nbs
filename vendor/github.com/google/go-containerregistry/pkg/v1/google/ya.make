GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auth.go
    doc.go
    keychain.go
    list.go
    options.go
)

GO_TEST_SRCS(list_test.go)

IF (ARCH_X86_64)
    GO_TEST_SRCS(auth_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
