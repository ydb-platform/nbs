# -*- coding: utf-8 -*-
from contrib.ydb.tests.oss.canonical import set_canondata_root

pytest_plugins = 'contrib.contrib.ydb.tests.library.fixtures'


def pytest_configure(config):
    set_canondata_root('contrib/ydb/tests/functional/topic/canondata')
