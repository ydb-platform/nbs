import os
import requests
import time

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


def request_tablet(tablet_id, params, attempt_count=10):
    mon_port = int(os.getenv("NFS_MON_PORT"))
    ce = None
    for i in range(attempt_count):
        try:
            response = requests.get(
                url=f"http://localhost:{mon_port}/tablets/app?"
                    f"TabletID={tablet_id}&{params}")
            ce = None
            response.raise_for_status()
            err_msg = f"Tablet pipe with {tablet_id} is not connected" \
                " with status: ERROR"
            if err_msg not in response.text:
                return response
        except requests.exceptions.ConnectionError as e:
            ce = e

        time.sleep(1)

    if ce:
        raise Exception(f"connection error: {ce}")

    raise Exception("tablet %s unreachable" % tablet_id)


def fetch_dir_viewer_entries(tablet_id, node_id):
    response = request_tablet(tablet_id, f"action=dirViewer&nodeId={node_id}")
    try:
        entries = response.json()["entries"]
    except Exception as e:
        raise Exception(f"invalid response, error: {e}, text: {response.text}")

    for entry in entries:
        del entry["node"]["shardNodeName"]
        del entry["node"]["id"]
    return entries


def fetch_locks(tablet_id):
    response = request_tablet(tablet_id, "action=locks&getContent=1")
    return response.json()
