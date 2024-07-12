GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    get_go_version.go
    go_build.go
    go_is_installed.go
    install_go_version.go
)

GO_TEST_SRCS(go_test.go)

END()

RECURSE(
    gotest
)
