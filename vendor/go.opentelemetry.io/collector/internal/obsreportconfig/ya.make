GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    obsreportconfig.go
)

GO_TEST_SRCS(obsreportconfig_test.go)

END()

RECURSE(
    gotest
    obsmetrics
)
