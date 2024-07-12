GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    longrunning.go
)

GO_TEST_SRCS(
    example_test.go
    longrunning_test.go
)

END()

RECURSE(
    autogen
    gotest
    internal
)
