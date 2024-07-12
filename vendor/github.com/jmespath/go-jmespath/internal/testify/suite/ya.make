GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

GO_SKIP_TESTS(
    TestSuiteRequireTwice
    TestSuiteRecoverPanic
)

SRCS(
    doc.go
    interfaces.go
    suite.go
)

GO_TEST_SRCS(suite_test.go)

END()

RECURSE(
    gotest
)
