GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config.go
    doc.go
    environment_variables.go
    guard.go
    helper.go
    util.go
    working_dir.go
)

GO_XTEST_SRCS(
    # working_dir_json_test.go
)

END()

RECURSE(
    gotest
)
