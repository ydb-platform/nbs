GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    rootcerts.go
)

GO_TEST_SRCS(rootcerts_test.go)

IF (OS_LINUX)
    SRCS(
        rootcerts_base.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        rootcerts_darwin.go
    )

    GO_TEST_SRCS(rootcerts_darwin_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        rootcerts_base.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
