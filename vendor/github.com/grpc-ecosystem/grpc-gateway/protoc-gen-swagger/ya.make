GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    main.go
)

GO_TEST_SRCS(main_test.go)

END()

RECURSE(
    genswagger
    gotest
    options
)
