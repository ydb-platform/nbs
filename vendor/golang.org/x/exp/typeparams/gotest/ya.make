GO_TEST_FOR(vendor/golang.org/x/exp/typeparams)

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

DATA(
    arcadia/vendor/golang.org/x/exp/typeparams
)

TEST_CWD(vendor/golang.org/x/exp/typeparams)

INCLUDE(${ARCADIA_ROOT}/library/go/test/go_toolchain/recipe.inc)

END()
