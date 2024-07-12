GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    sdnotify.go
    watchdog.go
)

GO_TEST_SRCS(
    sdnotify_test.go
    watchdog_test.go
)

END()

RECURSE(
    gotest
)
