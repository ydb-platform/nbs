GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(
        client_linux.go
    )
ENDIF()

END()
