PY3TEST()

FORK_TEST_FILES()
SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

ENV(MVP_META_BINARY="contrib/ydb/mvp/meta/bin/mvp_meta")

TEST_SRCS(
    conftest.py
    support_links_env.py
    test_support_links.py
)

DEPENDS(
    contrib/ydb/mvp/meta/bin
)

PEERDIR(
    contrib/python/requests
    contrib/ydb/tests/functional/mvp/common
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/ydb_sdk_import
)

END()
