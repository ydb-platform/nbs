# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from contrib.ydb.tests.library.harness.util import LogLevels
from contrib.ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            additional_log_configs={
                "CHANGE_EXCHANGE": LogLevels.DEBUG,
            },
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ]
        yatest.common.execute(cmd, wait=True)
