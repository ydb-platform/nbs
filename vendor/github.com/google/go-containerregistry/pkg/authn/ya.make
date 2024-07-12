GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    anon.go
    auth.go
    authn.go
    basic.go
    bearer.go
    doc.go
    keychain.go
    multikeychain.go
)

GO_TEST_SRCS(
    anon_test.go
    authn_test.go
    basic_test.go
    bearer_test.go
    keychain_test.go
    multikeychain_test.go
)

END()

RECURSE(
    github
    gotest
)
