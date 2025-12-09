#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from contrib.ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from contrib.ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403 Adding custom hooks for YQv2 support
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import AddInflightExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import AddDataInflightExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import AddFormatSizeLimitExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import DefaultConfigExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import YQv2Extension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import ComputeExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import StatsModeExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr


@pytest.fixture
def kikimr(request: pytest.FixtureRequest, yq_version: str):
    kikimr_extensions = [
        AddFormatSizeLimitExtension(),
        AddInflightExtension(),
        AddDataInflightExtension(),
        DefaultConfigExtension(''),
        YQv2Extension(yq_version),
        ComputeExtension(),
        StatsModeExtension(''),
    ]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


@pytest.fixture
def client(kikimr, request=None):
    client = FederatedQueryClient(
        request.param["folder_id"] if request is not None else "my_folder", streaming_over_kikimr=kikimr
    )
    return client
