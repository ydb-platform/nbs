GO_LIBRARY()

LICENSE(BSD-3-Clause)

IF (OS_WINDOWS)
    SRCS(
        config.go
        mgr.go
        recovery.go
        service.go
    )

    GO_XTEST_SRCS(mgr_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
