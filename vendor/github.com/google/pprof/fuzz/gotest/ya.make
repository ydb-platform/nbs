GO_TEST_FOR(vendor/github.com/google/pprof/fuzz)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/github.com/google/pprof/fuzz/testdata
)

TEST_CWD(vendor/github.com/google/pprof/fuzz)

END()
