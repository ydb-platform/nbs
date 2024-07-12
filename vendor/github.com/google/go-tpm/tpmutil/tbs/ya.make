GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

IF (OS_WINDOWS)
    SRCS(
        tbs_windows.go
    )
ENDIF()

END()
