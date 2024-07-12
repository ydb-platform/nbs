GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
)

IF (OS_LINUX)
    SRCS(
        cgroup.go
        cgroups.go
        errors.go
        mountpoint.go
        subsys.go
    )

    GO_TEST_SRCS(
        cgroup_test.go
        cgroups_test.go
        mountpoint_test.go
        subsys_test.go
        util_test.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
