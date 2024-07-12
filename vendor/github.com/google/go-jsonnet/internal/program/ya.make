GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    desugarer.go
    program.go
    static_analyzer.go
)

GO_TEST_SRCS(
    desugarer_test.go
    static_analyzer_test.go
)

END()

RECURSE(
    gotest
)
