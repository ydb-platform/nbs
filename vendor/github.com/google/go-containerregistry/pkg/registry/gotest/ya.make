GO_TEST_FOR(vendor/github.com/google/go-containerregistry/pkg/registry)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

# requires go command

INCLUDE(${ARCADIA_ROOT}/library/go/test/go_toolchain/recipe.inc)

END()
