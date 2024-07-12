GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    flatmap.go
    paths.go
    values.go
    values_equiv.go
)

GO_TEST_SRCS(
    flatmap_test.go
    paths_test.go
    values_equiv_test.go
    values_test.go
)

END()

RECURSE(
    gotest
)
