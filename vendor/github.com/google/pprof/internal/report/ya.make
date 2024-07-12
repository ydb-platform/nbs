GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    package.go
    report.go
    shortnames.go
    source.go
    source_html.go
    stacks.go
    synth.go
)

GO_TEST_SRCS(
    package_test.go
    report_test.go
    shortnames_test.go
    source_test.go
    stacks_test.go
    synth_test.go
)

END()

RECURSE(
    # gotest
)
