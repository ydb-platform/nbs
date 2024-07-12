GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    docker.go
)

GO_TEST_SRCS(main_test.go)

IF (OS_LINUX)
    SRCS(
        docker_linux.go
    )

    GO_TEST_SRCS(docker_linux_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        docker_notlinux.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        docker_notlinux.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
