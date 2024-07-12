GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    profile.go
)

GO_TEST_SRCS(profile_test.go)

GO_XTEST_SRCS(
    example_test.go
    trace_test.go
)

END()

RECURSE(
    # DISABLED invokes go build
    # gotest
)
