GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

GO_SKIP_TESTS(
    TestSimple
    TestInitGoEnv
    # call go binary
)

SRCS(
    setup.go
)

GO_TEST_SRCS(setup_test.go)

GO_XTEST_SRCS(script_test.go)

END()

RECURSE(
    gotest
)
