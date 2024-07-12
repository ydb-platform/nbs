GO_TEST_FOR(vendor/github.com/hashicorp/go-rootcerts)

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

DATA(
    arcadia/vendor/github.com/hashicorp/go-rootcerts
)

TEST_CWD(vendor/github.com/hashicorp/go-rootcerts)

GO_SKIP_TESTS(TestLoadCACertsFromDirWithSymlinks)

END()
