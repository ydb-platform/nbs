GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute_path.go
    attribute_path_step.go
    attribute_paths.go
    doc.go
)

GO_XTEST_SRCS(
    attribute_path_step_test.go
    attribute_path_test.go
    attribute_paths_test.go
)

END()

RECURSE(
    gotest
)
