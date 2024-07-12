GO_TEST_FOR(vendor/golang.org/x/text/message/pipeline)

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

DATA(
    arcadia/vendor/golang.org/x/text/message/pipeline/testdata
)

TEST_CWD(vendor/golang.org/x/text/message/pipeline)

INCLUDE(${ARCADIA_ROOT}/library/go/test/go_toolchain/recipe.inc)

END()
