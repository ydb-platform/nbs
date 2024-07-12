GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    semver.go
    sort.go
)

GO_TEST_SRCS(semver_test.go)

END()

RECURSE(
    gotest
)
