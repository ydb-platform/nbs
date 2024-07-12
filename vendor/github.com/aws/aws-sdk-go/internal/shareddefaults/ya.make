GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ecs_container.go
    shared_config.go
    shared_config_resolve_home_go1.12.go
)

IF (OS_LINUX)
    GO_XTEST_SRCS(shared_config_other_test.go)
ENDIF()

IF (OS_DARWIN)
    GO_XTEST_SRCS(shared_config_other_test.go)
ENDIF()

IF (OS_WINDOWS)
    GO_XTEST_SRCS(shared_config_windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
