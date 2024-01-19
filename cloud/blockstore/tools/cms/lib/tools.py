import cloud.blockstore.tools.cms.lib.cms as libcms
import cloud.blockstore.tools.cms.lib.config as libconfig

import logging
import os
import shutil
import tempfile

from google.protobuf.text_format import Parse as ProtoParse


def parse_cms_configs(items):
    result = {}

    for item in items:
        for c in item.Config.NamedConfigs:
            name = c.Name
            text = c.Config.decode('utf-8')
            result[name] = ProtoParse(text, libconfig.CONFIGS[name]())

    return result


class Scp(object):

    def __init__(self, pssh, test_data_dir=None):
        self.__pssh = pssh
        self.__dir = test_data_dir

    def execute(self, host, src, dst):
        self.__pssh.scp(host, src, dst)
        if self.__dir is not None:
            hostdir = os.path.join(self.__dir, 'files', host)
            os.makedirs(hostdir, exist_ok=True)
            for f in os.listdir(dst):
                shutil.copy(os.path.join(dst, f), hostdir)


class ScpMock(object):

    def __init__(self, test_data_dir):
        self.__dir = os.path.join(test_data_dir, "files")

    def execute(self, host, src, dst):
        # XXX src ignored
        hostdir = os.path.join(self.__dir, host)
        files = os.listdir(hostdir)
        for f in files:
            shutil.copy(os.path.join(hostdir, f), dst)


def get_config_files(host, scp, cfg_path):
    fs_configs = {}

    with tempfile.TemporaryDirectory() as dst:
        scp.execute(host, os.path.join(cfg_path, 'nbs-*.txt'), dst)

        for name, path in libconfig.CONFIG_NAMES.items():
            if not path:
                continue

            p = os.path.join(dst, path)
            if not os.path.exists(p):
                continue

            with open(p, 'rb') as f:
                s = f.read().decode("utf-8").strip()
                if s:
                    logging.debug(f'[get_config_files] {name} {path}: {s}')
                    message = ProtoParse(s,
                                         libconfig.CONFIGS[name](),
                                         allow_unknown_field=True)

                    fs_configs[name] = message

    return fs_configs


def find_items(response, config_names):
    items = []
    for item in response:
        assert item.Kind == libcms.NAMED_CONFIG_KIND

        for config in item.Config.NamedConfigs:
            if config.Name in config_names:
                items.append(item)
    return items
