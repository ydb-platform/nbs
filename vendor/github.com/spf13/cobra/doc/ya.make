GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    man_docs.go
    md_docs.go
    rest_docs.go
    util.go
    yaml_docs.go
)

GO_TEST_SRCS(
    cmd_test.go
    man_docs_test.go
    md_docs_test.go
    rest_docs_test.go
    yaml_docs_test.go
)

GO_XTEST_SRCS(man_examples_test.go)

END()

RECURSE(
    gotest
)
