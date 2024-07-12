GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_WINDOWS)
    SRCS(
        install.go
        log.go
    )

    GO_XTEST_SRCS(log_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
