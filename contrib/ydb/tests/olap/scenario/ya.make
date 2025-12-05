PY3TEST()

    TAG(ya:manual)

    TIMEOUT(600)

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS(
        test_simple.py
        test_scheme_load.py
        test_alter_tiering.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/pandas
        contrib/python/requests
        contrib/ydb/public/sdk/python
        contrib/ydb/public/sdk/python/enable_v3_new_behavior
        contrib/ydb/tests/olap/lib
        contrib/ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
    )

END()
