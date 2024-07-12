GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    exact_version.go
    latest_version.go
    releases.go
    versions.go
)

GO_TEST_SRCS(
    exact_version_test.go
    latest_version_test.go
    # releases_test.go
    versions_test.go
)

END()

RECURSE(
    gotest
)
