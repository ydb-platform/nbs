import logging
import os
import tempfile

import yatest.common as common

from subprocess import call

from google.protobuf.text_format import MessageToBytes

from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig, \
    TDiskRegistryProxyConfig, DISK_AGENT_BACKEND_AIO

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus


CFG_PREFIX = 'Cloud.NBS.'


class Device:

    def __init__(self, path, uuid, block_size, block_count, handle, storage_pool_name):
        self.path = path
        self.uuid = uuid
        self.block_size = block_size
        self.block_count = block_count
        self.handle = handle
        self.storage_pool_name = storage_pool_name


def get_shutdown_agent_interval():
    if common.context.sanitize is not None:
        return 30 * 1000
    return 0  # Use default


def enable_custom_cms_configs(client):
    req = msgbus.TConsoleRequest()
    configs_config = req.SetConfigRequest.Config.ConfigsConfig

    restrictions = configs_config.UsageScopeRestrictions

    restrictions.AllowedTenantUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)
    restrictions.AllowedNodeTypeUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def update_cms_config(client, name, config, node_type=None):
    req = msgbus.TConsoleRequest()
    action = req.ConfigureRequest.Actions.add()

    custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
    custom_cfg.Name = CFG_PREFIX + name
    custom_cfg.Config = MessageToBytes(config, as_one_line=True)

    s = action.AddConfigItem.ConfigItem.UsageScope

    s.TenantAndNodeTypeFilter.Tenant = '/Root/nbs'
    if node_type is not None:
        s.TenantAndNodeTypeFilter.NodeType = node_type

    action.AddConfigItem.ConfigItem.MergeStrategy = console.TConfigItem.MERGE

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def create_file_device(dir, device_id, block_size, block_count_per_device):
    tmp_file = tempfile.NamedTemporaryFile(suffix=".nonrepl", delete=False, dir=dir)

    tmp_file.seek(block_count_per_device * block_size - 1)
    tmp_file.write(b'\0')
    tmp_file.flush()

    return Device(
        path=tmp_file.name,
        uuid="FileDevice-" + str(device_id),
        block_size=block_size,
        block_count=block_count_per_device,
        handle=tmp_file,
        storage_pool_name=None,
    )


def create_memory_device(device_id, block_size, block_count_per_device):
    return Device(
        path=None,
        uuid="MemoryDevice-" + str(device_id),
        block_size=block_size,
        block_count=block_count_per_device,
        handle=None,
        storage_pool_name=None,
    )


def create_memory_devices(
        device_count,
        block_size,
        block_count_per_device):

    devices = []
    for i in range(device_count):
        devices.append(create_memory_device(
            i + 1,
            block_size,
            block_count_per_device
        ))

    return devices


def create_file_devices(
        dir,
        device_count,
        block_size,
        block_count_per_device):

    devices = []
    try:
        for i in range(device_count):
            devices.append(create_file_device(
                dir,
                i + 1,
                block_size,
                block_count_per_device
            ))
    except Exception:
        for d in devices:
            d.handle.close()
            os.unlink(d.path)
        raise

    return devices


def create_devices(
        use_memory_devices,
        device_count,
        block_size,
        block_count_per_device,
        ram_drive_path):

    devices = []

    if use_memory_devices:
        logging.info("Use memory devices for NRD")

        devices = create_memory_devices(
            device_count,
            block_size,
            block_count_per_device)
    else:
        logging.info("Use file devices for NRD")

        dir = ram_drive_path

        if dir is not None:
            logging.info("create device files on RAM disk at %s" % dir)
        else:
            logging.info("create device files in default TMP folder")

        try:
            devices = create_file_devices(
                dir,
                device_count,
                block_size,
                block_count_per_device)
        except Exception as e:
            logging.error("can't create device files: {}".format(e))

            if dir is None:
                raise

            logging.info("content of {}: {}".format(dir, os.listdir(dir)))

            logging.warn("fallback to default TMP folder")

            devices = create_file_devices(
                None,
                device_count,
                block_size,
                block_count_per_device)

        for device in devices:
            logging.info("device '%s' stores in %s" % (device.uuid, device.path))

    return devices


def setup_disk_registry_proxy_config(kikimr):
    config = TDiskRegistryProxyConfig()
    config.Owner = 16045690984503103501
    config.OwnerIdx = 2

    update_cms_config(kikimr, 'DiskRegistryProxyConfig', config)


def setup_disk_agent_config(
        kikimr,
        devices,
        device_erase_method,
        dedicated_disk_agent=False,
        agent_id="localhost",
        node_type='disk-agent',
        cached_sessions_path=None,
        backend=DISK_AGENT_BACKEND_AIO):

    config = TDiskAgentConfig()
    config.Enabled = True
    config.DedicatedDiskAgent = dedicated_disk_agent
    config.Backend = backend
    config.DirectIoFlagDisabled = True
    config.AgentId = agent_id
    config.AcquireRequired = True
    config.RegisterRetryTimeout = 1000  # 1 second
    config.ShutdownTimeout = get_shutdown_agent_interval()
    config.IOParserActorCount = 4
    config.OffloadAllIORequestsParsingEnabled = True
    config.IOParserActorAllocateStorageEnabled = True
    config.PathsPerFileIOService = 2
    config.MaxParallelSecureErasesAllowed = 3

    if cached_sessions_path is not None:
        config.CachedSessionsPath = cached_sessions_path
    if device_erase_method is not None:
        config.DeviceEraseMethod = device_erase_method

    for device in devices:
        if device.path is not None:
            arg = config.FileDevices.add()
            arg.Path = device.path
            arg.DeviceId = device.uuid
            arg.BlockSize = device.block_size
        else:
            arg = config.MemoryDevices.add()
            arg.Name = device.uuid
            arg.DeviceId = device.uuid
            arg.BlockSize = device.block_size
            arg.BlocksCount = device.block_count

        if device.storage_pool_name is not None:
            arg.PoolName = device.storage_pool_name

    update_cms_config(kikimr, 'DiskAgentConfig', config, node_type=node_type)


def make_agent_node_type(i):
    return "disk-agent-%s" % i if i > 0 else "disk-agent"


def make_agent_id(i):
    return "localhost-%s" % i if i > 0 else "localhost"


def make_agent_session_cache_path(cached_sessions_dir_path, i):
    if cached_sessions_dir_path is None:
        return None
    return os.path.join(cached_sessions_dir_path, "sessions-%s.cache.txt" % i)


class DeviceInfo:

    def __init__(self, uuid, serial_number=None):
        self.uuid = uuid
        self.serial_number = serial_number if serial_number else 'SN-' + uuid


class AgentInfo:

    def __init__(self, agent_id, devices):
        self.agent_id = agent_id
        self.devices = devices


def setup_disk_registry_config(
        agent_infos,
        nbs_port,
        nbs_client_binary_path):
    tmp_file = tempfile.NamedTemporaryFile(suffix=".nonrepl")

    # configuring rot pool with 1GiB allocation unit (small enough for all
    # scenarios)
    tmp_file.write(
        '''
        KnownDevicePools <
            Name: "rot"
            AllocationUnit: 1073741824
            Kind: DEVICE_POOL_KIND_GLOBAL
        >
        '''.encode("utf8"))

    for agent_info in agent_infos:
        tmp_file.write(b'KnownAgents {\n')
        tmp_file.write(b'AgentId: "%s"\n' % agent_info.agent_id.encode("utf8"))

        for device in agent_info.devices:
            tmp_file.write(
                f'''
                KnownDevices <
                    DeviceUUID: "{device.uuid}"
                    SerialNumber: "{device.serial_number}"
                >
                '''.encode("utf8"))

        tmp_file.write(b'}\n')
    tmp_file.flush()

    result = call(
        [nbs_client_binary_path,
            "UpdateDiskRegistryConfig",
            "--proto",
            "--input", tmp_file.name,
            "--host", "localhost",
            "--port", str(nbs_port)]
    )

    assert result == 0


def setup_disk_registry_config_simple(
        devices,
        nbs_port,
        nbs_client_binary_path):

    setup_disk_registry_config(
        [AgentInfo(make_agent_id(0), [DeviceInfo(d.uuid) for d in devices])],
        nbs_port,
        nbs_client_binary_path
    )


def enable_writable_state(nbs_port, nbs_client_binary_path):
    result = call(
        [nbs_client_binary_path,
            "ExecuteAction",
            "--action", "DiskRegistrySetWritableState",
            "--input-bytes", '{"State": true}',
            "--host", "localhost",
            "--port", str(nbs_port)]
    )

    assert result == 0


def setup_nonreplicated(
        kikimr_client,
        devices_per_agent,
        device_erase_method=None,
        dedicated_disk_agent=False,
        agent_count=1,
        cached_sessions_dir_path=None,
        backend=DISK_AGENT_BACKEND_AIO):

    enable_custom_cms_configs(kikimr_client)
    setup_disk_registry_proxy_config(kikimr_client)
    for i in range(agent_count):
        setup_disk_agent_config(
            kikimr_client,
            devices_per_agent[i],
            device_erase_method,
            dedicated_disk_agent,
            agent_id=make_agent_id(i),
            node_type=make_agent_node_type(i),
            cached_sessions_path=make_agent_session_cache_path(cached_sessions_dir_path, i),
            backend=backend)
