GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestDestinationsWithDifferentFlags)

GO_XTEST_SRCS(klog_test.go)

END()

RECURSE(
    internal
)
