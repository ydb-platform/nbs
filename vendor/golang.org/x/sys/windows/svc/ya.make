GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_WINDOWS)
    SRCS(
        security.go
        service.go
    )

    GO_XTEST_SRCS(svc_test.go)
ENDIF()

END()

RECURSE(
    gotest
)

IF (OS_WINDOWS)
    RECURSE(
        debug
        mgr
        example
        eventlog
    )
ENDIF()
