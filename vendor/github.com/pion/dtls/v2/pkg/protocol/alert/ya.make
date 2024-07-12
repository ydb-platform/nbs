GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    alert.go
)

GO_TEST_SRCS(alert_test.go)

END()

RECURSE(
    gotest
)
