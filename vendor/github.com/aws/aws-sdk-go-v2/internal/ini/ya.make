GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    errors.go
    go_module_metadata.go
    ini.go
    parse.go
    sections.go
    strings.go
    token.go
    tokenize.go
    value.go
)

GO_TEST_SRCS(ini_test.go)

END()

RECURSE(
    gotest
)
