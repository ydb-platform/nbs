GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    exec.go
    fixup_go119.go
)

GO_TEST_SRCS(
    # exec_test.go
)

GO_XTEST_SRCS(
    new_test.go
    stdiopipe_test.go
)

END()

RECURSE(
    gotest
    testing
)
