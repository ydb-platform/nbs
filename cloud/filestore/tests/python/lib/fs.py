import os
import requests

import yatest.common as common


def __write_some_data(client, fs_id, path, data):
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write("data for %s" % path)
        f.write(":: actual data: %s" % data)

    client.write(fs_id, path, "--data", data_file)


DIR = 1
FILE = 2
SYMLINK = 3


class FsItem:

    def __init__(self, path, node_type, data):
        self.path = path
        self.node_type = node_type
        self.data = data


def fill_fs(client, fs_id, items):
    for item in items:
        if item.node_type == DIR:
            client.mkdir(fs_id, item.path)
        elif item.node_type == FILE:
            if item.data is not None:
                __write_some_data(client, fs_id, item.path, item.data)
            else:
                client.touch(fs_id, item.path)
        else:
            client.ln(fs_id, item.path, "--symlink", item.data)


def fetch_dir_viewer_entries(tablet_id, node_id):
    mon_port = os.getenv("NFS_MON_PORT")
    response = requests.get(
        url=f"http://localhost:{mon_port}/tablets/app?"
            f"TabletID={tablet_id}&action=dirViewer&nodeId={node_id}")
    response.raise_for_status()
    entries = response.json()["entries"]
    for entry in entries:
        del entry["node"]["shardNodeName"]
        del entry["node"]["id"]
    return entries


def fetch_locks(tablet_id):
    mon_port = os.getenv("NFS_MON_PORT")
    response = requests.get(
        url=f"http://localhost:{mon_port}/tablets/app?"
            f"TabletID={tablet_id}&action=locks&getContent=1")
    response.raise_for_status()
    return response.json()
