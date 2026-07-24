import os

from google.protobuf import text_format

from cloud.filestore.config.server_pb2 import TServerAppConfig
from cloud.filestore.tests.python.lib.daemon_config import (
    FilestoreServerConfigGenerator,
    FilestoreVhostConfigGenerator,
)

from contrib.ydb.core.protos import config_pb2


def test_blob_storage_config():
    for generator_type in (
        FilestoreServerConfigGenerator,
        FilestoreVhostConfigGenerator,
    ):
        configurator = generator_type(
            binary_path="filestore",
            app_config=TServerAppConfig(),
            service_type="kikimr",
            verbose=False,
            kikimr_port=1,
            domain="Root",
            bs_failure_probability=0.25,
        )
        configurator.generate_configs(
            config_pb2.TDomainsConfig(),
            config_pb2.TStaticNameserviceConfig(),
        )

        bs_config_path = os.path.join(configurator.configs_dir, "bs.txt")
        with open(bs_config_path) as config_file:
            generated_config = text_format.Parse(
                config_file.read(),
                config_pb2.TBlobStorageConfig(),
            )

        expected_config = config_pb2.TBlobStorageConfig()
        failure_injection_config = (
            expected_config.ServiceSet.FailureInjectionConfig
        )
        failure_injection_config.FailureProbability = 0.25
        assert generated_config == expected_config

        command = configurator.generate_command()
        bs_file_arg = command.index("--bs-file")
        assert command[bs_file_arg + 1] == bs_config_path


def test_blob_storage_config_is_not_generated_by_default():
    configurator = FilestoreServerConfigGenerator(
        binary_path="filestore",
        app_config=TServerAppConfig(),
        service_type="kikimr",
        verbose=False,
        kikimr_port=1,
        domain="Root",
    )
    configurator.generate_configs(
        config_pb2.TDomainsConfig(),
        config_pb2.TStaticNameserviceConfig(),
    )

    bs_config_path = os.path.join(configurator.configs_dir, "bs.txt")
    assert not os.path.exists(bs_config_path)
    command = configurator.generate_command()
    assert "--bs-file" not in command
