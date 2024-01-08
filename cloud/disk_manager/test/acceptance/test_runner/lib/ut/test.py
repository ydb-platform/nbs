import yatest.common

from pathlib import Path

from cloud.blockstore.pylibs import common
from cloud.disk_manager.test.acceptance.test_runner.lib import create_ycp


_current_file = __file__


def test_create_ycp():
    current_file_path = Path(yatest.common.source_path()) / _current_file
    cluster_test_configs_path = current_file_path.parent / "test-configs"
    _ = create_ycp(
        cluster_name='testing',
        zone_id='sas',
        chunk_storage_type='ydb',
        cluster_test_config_path=cluster_test_configs_path,
        use_generated_config=False,
        make_ycp_config_generator=common.make_config_generator_stub,
        module_factory=common.ModuleFactories(
            common.make_test_result_processor_stub,
            common.fetch_server_version_stub,
            common.make_config_generator_stub,
            make_ssh_client=common.make_ssh_client,
            make_helpers=common.make_helpers,
            make_sftp_client=common.make_sftp_client,
            make_ssh_channel=common.make_ssh_channel,
        ),
        dry_run=True)
