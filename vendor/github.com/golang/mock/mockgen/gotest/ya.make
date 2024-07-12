GO_TEST_FOR(vendor/github.com/golang/mock/mockgen)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/github.com/golang/mock/mockgen/internal
)

TEST_CWD(vendor/github.com/golang/mock/mockgen)

INCLUDE(${ARCADIA_ROOT}/library/go/test/go_toolchain/recipe.inc)

GO_SKIP_TESTS(
    Test_createPackageMap
    TestFileParser_ParseFile
    TestFileParser_ParsePackage
    TestImportsOfFile
)

END()
