import jinja2
import json
import os

from dataclasses import dataclass
from typing import Dict

from library.python import resource


@dataclass
class FolderDesc:
    folder_id: str
    zone_id: str
    subnet_name: str
    subnet_id: str
    image_name: str
    image_folder_id: str
    filesystem_id: str
    platform_id: str = None
    service_account_id: str = None
    symmetric_key_id: str = None
    hot_attach_fs: bool = True
    create_underlay_vms: bool = False


@dataclass
class ClusterTestConfig:
    name: str
    solomon_cluster: str
    ipc_type_to_folder_id: Dict[str, str]
    chunk_storage_type_to_folder_id: Dict[str, str]
    folder_desc: Dict[str, FolderDesc]

    def ipc_type_to_folder_desc(self, ipc):
        folder_id = self.ipc_type_to_folder_id.get(ipc)
        if folder_id is None:
            return None
        return self.folder_desc.get(folder_id)

    def chunk_storage_type_to_folder_desc(self, chunk_storage_type):
        folder_id = self.chunk_storage_type_to_folder_id.get(chunk_storage_type)
        if folder_id is None:
            return None
        return self.folder_desc.get(folder_id)

    def get_solomon_cluster(self, host: str) -> str:
        return self.solomon_cluster + '_' + host[:3]


def get_cluster_test_config(name: str, zone_id: str, config_path: str = None) -> ClusterTestConfig:
    if config_path is not None:
        with open(os.path.join(config_path, f'{name}.json'), 'r') as f:
            template = f.read()
    else:
        r = resource.find(f'{name}.json')
        if not r:
            raise FileNotFoundError(f"Cannot find ya.make resource with name '{name}.json'")
        template = r.decode('utf8')
    cfg = json.loads(jinja2.Template(template).render(zone_id=zone_id))
    res = ClusterTestConfig(None, None, None, None, None)
    res.__dict__.update(cfg)
    folders = dict()
    for name in cfg['folder_desc']:
        folder = FolderDesc(
            folder_id=None,
            zone_id=None,
            subnet_name=None,
            subnet_id=None,
            image_name=None,
            image_folder_id=None,
            filesystem_id=None,
        )
        folder.__dict__.update(cfg['folder_desc'][name])
        folders[name] = folder
    res.__dict__.update({
        'folder_desc': folders
    })
    return res


def translate_disk_type(cluster: str, disk_type: str) -> str:
    return disk_type
