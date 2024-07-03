import json
import os
import signal
import subprocess
import tempfile
import yatest.common as yatest_common


def __read_json(f):
    line = f.readline().strip()
    return json.loads(line)


def __wait_message(f, msg):
    while True:
        entry = __read_json(f)
        if entry and entry['message'].find(msg) != -1:
            break


def __read_comm(pid):
    comm_path = os.path.join('/proc', str(pid), 'comm')
    with open(comm_path, 'r') as f:
        return f.readline()


def test_vhost_server():
    binary_path = yatest_common.binary_path("cloud/blockstore/vhost-server/blockstore-vhost-server")
    data_file_size = 4096
    data_file_path = os.path.join(
        yatest_common.output_path(),
        'pci-0000:00:16.0-sas-phy2-lun-0')

    out_r_fd, out_w_fd = os.pipe()
    err_r_fd, err_w_fd = os.pipe()

    with tempfile.TemporaryDirectory() as tmp_dir, \
            os.fdopen(out_r_fd, 'r') as out_r, \
            os.fdopen(out_w_fd, 'w') as out_w, \
            os.fdopen(err_r_fd, 'r') as err_r, \
            os.fdopen(err_w_fd, 'w') as err_w:

        socket_path = os.path.join(tmp_dir, 'local0.vhost')

        assert not os.path.exists(socket_path)

        with open(data_file_path, 'wb') as f:
            f.seek(data_file_size)
            f.write(b'\0')
            f.flush()

        server = subprocess.Popen(
            [
                binary_path,
                '-i', 'local0',
                '--disk-id', 'volume_with_long_name',
                '-s', socket_path,
                '-q', '2',
                '--device', f"{data_file_path}:{data_file_size}:0",
            ],
            stdin=None,
            stdout=out_w,
            stderr=err_w,
            bufsize=0,
            universal_newlines=True)

        __wait_message(err_r, 'Server started')

        assert os.path.exists(socket_path)

        assert 'vhost-volume_wi\n' == __read_comm(server.pid)

        for _ in range(3):
            server.send_signal(signal.SIGUSR1)
            s = __read_json(out_r)
            assert s['elapsed_ms'] != 0

        server.send_signal(signal.SIGUSR1)
        server.send_signal(signal.SIGINT)

        server.communicate()

        assert server.returncode == 0
        assert not os.path.exists(socket_path)

        __wait_message(err_r, 'Server has been stopped')
