GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

GO_SKIP_TESTS(
    TestTestwork
    # calls go binary
    TestScripts
    # TestScripts/evalsymlink executes pwd, which is not in $PATH
)

SRCS(
    cmd.go
    doc.go
    exe.go
    testscript.go
)

GO_TEST_SRCS(testscript_test.go)

IF (OS_LINUX)
    SRCS(
        clonefile.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        clonefile_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        clonefile_other.go
    )
ENDIF()

END()

RECURSE(
    gotest
    internal
)
