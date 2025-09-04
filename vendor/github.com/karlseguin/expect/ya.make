GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.0.8)

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
