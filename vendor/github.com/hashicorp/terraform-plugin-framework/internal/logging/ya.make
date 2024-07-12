GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    context.go
    doc.go
    environment_variables.go
    framework.go
    keys.go
)

GO_XTEST_SRCS(
    context_test.go
    framework_test.go
)

END()

RECURSE(
    gotest
)
