GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    latest_version.go
)

GO_TEST_SRCS(latest_version_test.go)

END()

RECURSE(
    gotest
)
