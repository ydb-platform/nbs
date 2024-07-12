GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    builder.go
    colelem.go
    contract.go
    order.go
    table.go
    trie.go
)

GO_TEST_SRCS(
    builder_test.go
    colelem_test.go
    contract_test.go
    order_test.go
    trie_test.go
)

END()

RECURSE(
    gotest
)
