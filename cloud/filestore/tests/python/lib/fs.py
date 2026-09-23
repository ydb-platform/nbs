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


def __is_transient_tablet_error(tablet_id, text):
    # These are the replies of ydb's TTabletMonitoringProxy (see
    # contrib/ydb/core/tablet/tablet_monitoring_proxy.cpp) for the cases when
    # the tablet pipe couldn't be established or got destroyed before the
    # tablet replied, e.g. when the tablet is being restarted after a config
    # change. Both are transient - the request should simply be retried.
    transient_errors = [
        f"Tablet pipe with {tablet_id} is not connected",
        "Tablet pipe is reset",
    ]
    return any(err in text for err in transient_errors)


def request_tablet(tablet_id, params, attempt_count=10):
    mon_port = int(os.getenv("NFS_MON_PORT"))
    last_error = None
    for i in range(attempt_count):
        try:
            response = requests.get(
                url=f"http://localhost:{mon_port}/tablets/app?"
                    f"TabletID={tablet_id}&{params}")
            response.raise_for_status()
            if not __is_transient_tablet_error(tablet_id, response.text):
                return response
            last_error = f"transient tablet error: {response.text}"
        except requests.exceptions.ConnectionError as e:
            last_error = f"connection error: {e}"

        time.sleep(1)

    raise Exception(
        f"tablet {tablet_id} unreachable after {attempt_count} attempts,"
        f" last error: {last_error}")


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
