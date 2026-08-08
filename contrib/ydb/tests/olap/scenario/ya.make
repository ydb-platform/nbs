PY3TEST()

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS(
        test_simple.py
        test_scheme_load.py
        test_alter_tiering.py
        test_insert.py
        test_alter_compression.py
    )

    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
    DEPENDS(
        )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/boto3
        contrib/python/requests
        contrib/python/moto
        contrib/python/Flask
        contrib/python/Flask-Cors
        contrib/ydb/public/sdk/python
        contrib/ydb/public/sdk/python/enable_v3_new_behavior
        contrib/ydb/tests/olap/lib
        contrib/ydb/tests/library
        contrib/ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
        library/recipes/common
    )

    SIZE(MEDIUM)

END()
