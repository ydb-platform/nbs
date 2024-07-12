GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    expression.go
    expression_step.go
    expression_step_attribute_name_exact.go
    expression_step_element_key_int_any.go
    expression_step_element_key_int_exact.go
    expression_step_element_key_string_any.go
    expression_step_element_key_string_exact.go
    expression_step_element_key_value_any.go
    expression_step_element_key_value_exact.go
    expression_step_parent.go
    expression_steps.go
    expressions.go
    path.go
    path_step.go
    path_step_attribute_name.go
    path_step_element_key_int.go
    path_step_element_key_string.go
    path_step_element_key_value.go
    path_steps.go
    paths.go
)

GO_XTEST_SRCS(
    expression_step_attribute_name_exact_test.go
    expression_step_element_key_int_any_test.go
    expression_step_element_key_int_exact_test.go
    expression_step_element_key_string_any_test.go
    expression_step_element_key_string_exact_test.go
    expression_step_element_key_value_any_test.go
    expression_step_element_key_value_exact_test.go
    expression_step_parent_test.go
    expression_steps_test.go
    expression_test.go
    expressions_test.go
    path_step_attribute_name_test.go
    path_step_element_key_int_test.go
    path_step_element_key_string_test.go
    path_step_element_key_value_test.go
    path_steps_test.go
    path_test.go
    paths_test.go
)

END()

RECURSE(
    gotest
)
