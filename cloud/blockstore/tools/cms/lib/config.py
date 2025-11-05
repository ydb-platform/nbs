from contrib.ydb.core.protos.auth_pb2 import TAuthConfig
from contrib.ydb.core.protos.config_pb2 import TActorSystemConfig, TLogConfig, TInterconnectConfig

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig
from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig, TDiskRegistryProxyConfig
from cloud.blockstore.config.logbroker_pb2 import TLogbrokerConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig
from cloud.blockstore.config.spdk_pb2 import TSpdkEnvConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.storage.core.config.features_pb2 import TFeaturesConfig

CONFIG_NAMES = {
    'ActorSystemConfig': "nbs-sys.txt",
    'AuthConfig': "nbs-auth.txt",
    'DiagnosticsConfig': "nbs-diag.txt",
    'DiscoveryServiceConfig': "nbs-discovery.txt",
    'DiskAgentConfig': None,
    'DiskRegistryProxyConfig': "nbs-disk-registry-proxy.txt",
    'FeaturesConfig': "nbs-features.txt",
    'InterconnectConfig': "nbs-ic.txt",
    'LogbrokerConfig': None,
    'LogConfig': "nbs-log.txt",
    'ServerAppConfig': "nbs-server.txt",
    'SpdkEnvConfig': None,
    'StorageServiceConfig': "nbs-storage.txt",
}

CONFIGS = {
    c.__name__[1:]: c for c in [
        TActorSystemConfig,
        TAuthConfig,
        TDiagnosticsConfig,
        TDiscoveryServiceConfig,
        TDiskAgentConfig,
        TDiskRegistryProxyConfig,
        TFeaturesConfig,
        TInterconnectConfig,
        TLogbrokerConfig,
        TLogConfig,
        TServerAppConfig,
        TServerConfig,
        TSpdkEnvConfig,
        TStorageServiceConfig,
    ]
}
