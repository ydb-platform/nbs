GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

# NB: transitive dependencies
# See https://github.com/golang/go/issues/29258 and st/YMAKE-102 for more details

SRCS(
    doc.go
    image.go
    index.go
    mutate.go
    rebase.go
)

GO_TEST_SRCS(whiteout_test.go)

GO_XTEST_SRCS(
    # index_test.go
    # mutate_test.go
    # rebase_test.go
)

END()

RECURSE(
    gotest
)
