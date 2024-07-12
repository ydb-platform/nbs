GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    installer.go
)

GO_XTEST_SRCS(
    installer_examples_test.go
    installer_test.go
)

END()

RECURSE(
    build
    checkpoint
    errors
    fs
    gotest
    internal
    product
    releases
    src
    version
)
