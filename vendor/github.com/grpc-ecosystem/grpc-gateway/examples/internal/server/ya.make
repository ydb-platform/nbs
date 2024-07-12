GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    a_bit_of_everything.go
    echo.go
    fieldmask_helper.go
    flow_combination.go
    main.go
    non_standard_names.go
    responsebody.go
    unannotatedecho.go
)

GO_TEST_SRCS(fieldmask_helper_test.go)

END()

RECURSE(
    gotest
)
