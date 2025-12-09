PY3_LIBRARY()

    PY_SRCS (
        __init__.py
        scenario_tests_helper.py
        data_generators.py
        table_helper.py
        drop_helper.py
        thread_helper.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/ydb/tests/olap/lib
        library/python/testing/yatest_common
        contrib/ydb/public/sdk/python
    )

END()
