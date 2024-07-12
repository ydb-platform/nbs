GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    git_revision.go
)

GO_TEST_SRCS(git_revision_test.go)

END()

RECURSE(
    gotest
)
