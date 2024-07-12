GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    flightcontrol.go
)

GO_TEST_SRCS(flightcontrol_test.go)

END()

RECURSE(
    gotest
)
