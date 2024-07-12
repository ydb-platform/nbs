GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    obsreporttest.go
    otelprometheuschecker.go
)

GO_TEST_SRCS(otelprometheuschecker_test.go)

GO_XTEST_SRCS(obsreporttest_test.go)

END()

RECURSE(
    gotest
)
