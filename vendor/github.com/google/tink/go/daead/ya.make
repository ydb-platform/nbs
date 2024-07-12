GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aes_siv_key_manager.go
    daead.go
    daead_factory.go
    daead_key_templates.go
)

GO_XTEST_SRCS(
    aes_siv_key_manager_test.go
    daead_factory_test.go
    daead_key_templates_test.go
    daead_test.go
)

END()

RECURSE(
    gotest
    subtle
)
