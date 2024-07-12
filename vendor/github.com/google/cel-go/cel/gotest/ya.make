GO_TEST_FOR(vendor/github.com/google/cel-go/cel)

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

DATA(
    arcadia/vendor/github.com/google/cel-go/cel/testdata
)

TEST_CWD(vendor/github.com/google/cel-go/cel)

END()
