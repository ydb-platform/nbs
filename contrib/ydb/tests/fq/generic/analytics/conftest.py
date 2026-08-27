import pytest

from contrib.ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from contrib.ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403 Adding custom hooks for YQv2 support
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import ConnectorExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import YQv2Extension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import TokenAccessorExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import MDBExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import YdbMvpExtension
from contrib.ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr

from contrib.ydb.tests.fq.generic.utils.settings import Settings

from typing import Final

docker_compose_file_path: Final = "contrib/ydb/tests/fq/generic/analytics/docker-compose.yml"


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_file_path=docker_compose_file_path)


@pytest.fixture
def mvp_external_ydb_endpoint(request) -> str:
    return request.param["endpoint"] if request is not None and hasattr(request, "param") else None


@pytest.fixture
def kikimr(request: pytest.FixtureRequest, settings: Settings, yq_version: str, mvp_external_ydb_endpoint: str):
    kikimr_extensions = [
        ConnectorExtension(settings.connector.grpc_host, settings.connector.grpc_port, False),
        TokenAccessorExtension(settings.token_accessor_mock.endpoint, settings.token_accessor_mock.hmac_secret_file),
        MDBExtension(settings.mdb_mock.endpoint),
        YdbMvpExtension(mvp_external_ydb_endpoint),
        YQv2Extension(yq_version),
    ]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


@pytest.fixture
def fq_client(kikimr, request=None) -> FederatedQueryClient:
    client = FederatedQueryClient(
        request.param["folder_id"] if request is not None else "my_folder", streaming_over_kikimr=kikimr
    )
    return client
