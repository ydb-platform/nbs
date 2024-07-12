SUBSCRIBER(g:go-contrib)

IF (OS_LINUX)
    RECURSE(
        client
        server
    )
ENDIF()
