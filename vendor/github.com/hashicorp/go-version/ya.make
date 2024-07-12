GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    constraint.go
    version.go
    version_collection.go
)

GO_TEST_SRCS(
    constraint_test.go
    version_collection_test.go
    version_test.go
)

END()

RECURSE(
    gotest
)
