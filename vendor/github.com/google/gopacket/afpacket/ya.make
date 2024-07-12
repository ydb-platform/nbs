GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_LINUX)
    SRCS(
        options.go
        sockopt_linux.go
    )

    GO_TEST_SRCS(afpacket_test.go)
ENDIF()

IF (OS_LINUX AND CGO_ENABLED)
    CGO_SRCS(
        afpacket.go
        header.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
