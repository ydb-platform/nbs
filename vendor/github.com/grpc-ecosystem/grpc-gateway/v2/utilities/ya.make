GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    pattern.go
    readerfactory.go
    string_array_flag.go
    trie.go
)

GO_XTEST_SRCS(
    string_array_flag_test.go
    trie_test.go
)

END()

RECURSE(
    gotest
)
