GO_TEST_FOR(vendor/github.com/google/tink/go/hybrid/subtle)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/github.com/google/wycheproof
)

ENV(TEST_SRCDIR=.)

TEST_CWD(vendor/github.com/google)

END()
