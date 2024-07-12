GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    mock.go
    vendor_dep.go
)

END()

RECURSE(
    source_mock_package
)
