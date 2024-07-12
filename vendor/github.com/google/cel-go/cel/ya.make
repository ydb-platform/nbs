GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    cel.go
    decls.go
    env.go
    folding.go
    inlining.go
    io.go
    library.go
    macro.go
    optimizer.go
    options.go
    program.go
    validator.go
)

GO_TEST_SRCS(
    cel_test.go
    decls_test.go
    env_test.go
    folding_test.go
    io_test.go
    validator_test.go
)

GO_XTEST_SRCS(
    cel_example_test.go
    inlining_test.go
    # optimizer_test.go
)

END()

RECURSE(
    gotest
)
