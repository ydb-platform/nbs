GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    copy_modifier.go
    forwarded_modifier.go
    framing_modifier.go
    header_append_modifier.go
    header_blacklist_modifier.go
    header_filter.go
    header_matcher.go
    header_modifier.go
    header_value_regex_filter.go
    header_verifier.go
    hopbyhop_modifier.go
    id_modifier.go
    via_modifier.go
)

GO_TEST_SRCS(
    copy_modifier_test.go
    forwarded_modifier_test.go
    framing_modifier_test.go
    header_append_modifier_test.go
    header_blacklist_modifier_test.go
    header_filter_test.go
    header_modifier_test.go
    header_verifier_test.go
    hopbyhop_modifier_test.go
    id_modifier_test.go
    via_modifier_test.go
)

END()

RECURSE(
    gotest
)
