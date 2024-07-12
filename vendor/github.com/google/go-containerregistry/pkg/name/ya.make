GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    check.go
    digest.go
    doc.go
    errors.go
    options.go
    ref.go
    registry.go
    repository.go
    tag.go
)

GO_TEST_SRCS(
    digest_test.go
    errors_test.go
    ref_test.go
    registry_test.go
    repository_test.go
    tag_test.go
)

END()

RECURSE(
    gotest
)
