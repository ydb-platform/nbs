GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    aliases.go
    environment_variables.go
    json.go
    plugin.go
    state_shim.go
    testcase_providers.go
    testcase_validate.go
    testing.go
    testing_config.go
    testing_new.go
    testing_new_config.go
    testing_new_import_state.go
    testing_new_refresh_state.go
    testing_sets.go
    teststep_providers.go
    teststep_validate.go
)

GO_TEST_SRCS(
    # plugin_test.go
    # testcase_providers_test.go
    # testcase_validate_test.go
    # testing_new_config_test.go
    # testing_new_import_state_test.go
    # testing_new_test.go
    # testing_sets_test.go
    # testing_test.go
    # teststep_providers_test.go
    # teststep_validate_test.go
)

GO_XTEST_SRCS(
    testing_example_test.go
    testing_sets_example_test.go
)

END()

RECURSE(
    gotest
)
