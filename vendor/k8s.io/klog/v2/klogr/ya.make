GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    klogr.go
)

GO_TEST_SRCS(klogr_test.go)

GO_XTEST_SRCS(output_test.go)

END()

RECURSE(
    calldepth-test
    gotest
)
