GO_TEST_FOR(vendor/go.uber.org/atomic)

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

DATA(
    arcadia/vendor/go.uber.org/atomic
)

TEST_CWD(vendor/go.uber.org/atomic)

INCLUDE(${ARCADIA_ROOT}/library/go/test/go_toolchain/recipe.inc)

END()
