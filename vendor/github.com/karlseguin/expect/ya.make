GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    comparitors.go
    expect.go
    posthandler.go
    reflection.go
    runner.go
)

GO_TEST_SRCS(
    each_no_test.go
    each_test.go
    expect_numeric_test.go
    expect_test.go
)

END()

RECURSE(
    gotest
)
