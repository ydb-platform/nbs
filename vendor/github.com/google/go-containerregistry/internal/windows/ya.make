GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    windows.go
)

GO_TEST_SRCS(windows_test.go)

END()

RECURSE(
    gotest
)
