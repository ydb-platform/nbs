import typing as tp


pytest_plugins: tp.Final[list[str]] = ["contrib.contrib.ydb.tests.library.fixtures"]
"""
Enable all the fixtures from the ydb.tests.library.fixtures file.
"""
