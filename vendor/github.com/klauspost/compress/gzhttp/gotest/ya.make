GO_TEST_FOR(vendor/github.com/klauspost/compress/gzhttp)

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

DATA(
    arcadia/vendor/github.com/klauspost/compress/gzhttp/testdata
)

TEST_CWD(vendor/github.com/klauspost/compress/gzhttp)

END()
