GO_LIBRARY()

SRCS(
    file.go
    pe.go
    section.go
    string.go
    symbol.go
)

GO_TEST_SRCS(
    file_cgo_test.go
    file_test.go
    symbols_test.go
)

END()

RECURSE(
)
