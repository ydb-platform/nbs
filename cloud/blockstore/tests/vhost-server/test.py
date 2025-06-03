import datetime
import json
import os
import signal
import subprocess
import tempfile
import yatest.common as yatest_common


VHOST_SERVER_BINARY_PATH = yatest_common.binary_path(
    "cloud/blockstore/vhost-server/blockstore-vhost-server")
RUN_AND_DIE_BINARY_PATH = yatest_common.binary_path(
    "cloud/blockstore/tests/vhost-server/run_and_die/run-and-die")


def __read_json(f):
    line = f.readline().strip()
    print(">", datetime.datetime.now(), line)
    try:
        return json.loads(line)
    except Exception:
        print("Read not a json line: ", line)
        return None


def __wait_message(f, msg):
    while True:
        entry = __read_json(f)
        if entry and entry['message'].find(msg) != -1:
            break


def __read_comm(pid):
    comm_path = os.path.join('/proc', str(pid), 'comm')
    with open(comm_path, 'r') as f:
        return f.readline()


def __prepare_data_file(data_file_size):
    data_file_path = os.path.join(
        yatest_common.output_path(),
        'pci-0000:00:16.0-sas-phy2-lun-0')
    with open(data_file_path, 'wb') as f:
        f.seek(data_file_size)
        f.write(b'\0')
        f.flush()
    return data_file_path


def test_vhost_server():
    data_file_size = 4096
    data_file_path = __prepare_data_file(data_file_size)

    out_r_fd, out_w_fd = os.pipe()
    err_r_fd, err_w_fd = os.pipe()

    with tempfile.TemporaryDirectory() as tmp_dir, \
            os.fdopen(out_r_fd, 'r') as out_r, \
            os.fdopen(out_w_fd, 'w') as out_w, \
            os.fdopen(err_r_fd, 'r') as err_r, \
            os.fdopen(err_w_fd, 'w') as err_w:

        socket_path = os.path.join(tmp_dir, 'local0.vhost')

        assert not os.path.exists(socket_path)

        server = subprocess.Popen(
            [
                VHOST_SERVER_BINARY_PATH,
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

        # check comm string
        assert 'vhost-volume_wi\n' == __read_comm(server.pid)

        # check pgid of server differs from its parent pgid
        assert os.getpgid(server.pid) != os.getpgid(os.getpid())

        # check stat request
        for _ in range(3):
            server.send_signal(signal.SIGUSR1)
            s = __read_json(out_r)
            assert s['failed'] == 0 and s['completed'] == 0

        server.send_signal(signal.SIGUSR1)
        server.send_signal(signal.SIGINT)

        server.communicate()

        assert server.returncode == 0
        assert not os.path.exists(socket_path)

        __wait_message(err_r, 'Server has been stopped')


def test_vhost_server_work_along():
    work_along_time = 5
    data_file_size = 4096
    data_file_path = __prepare_data_file(data_file_size)

    out_r_fd, out_w_fd = os.pipe()
    err_r_fd, err_w_fd = os.pipe()

    with tempfile.TemporaryDirectory() as tmp_dir, \
            os.fdopen(out_r_fd, 'r') as out_r, \
            os.fdopen(out_w_fd, 'w') as out_w, \
            os.fdopen(err_r_fd, 'r') as err_r, \
            os.fdopen(err_w_fd, 'w') as err_w:

        socket_path = os.path.join(tmp_dir, 'local0.vhost')

        assert not os.path.exists(socket_path)

        # prepare vhost command line
        vhost_cmd = [
            VHOST_SERVER_BINARY_PATH,
            '-i', 'local0',
            '--disk-id', 'volume_with_long_name',
            '-s', socket_path,
            '-q', '2',
            '--device', f"{data_file_path}:{data_file_size}:0",
            '--wait-after-parent-exit', str(work_along_time)
        ]

        # run proxy process that will launch our vhost-server
        proxy = subprocess.Popen(
            [
                RUN_AND_DIE_BINARY_PATH,
                '--cmd', json.dumps(vhost_cmd)
            ],
            stdin=None,
            stdout=out_w,
            stderr=err_w,
            bufsize=0,
            universal_newlines=True)

        # wait proxy exit
        proxy.communicate()
        assert proxy.returncode == 0

        # get pid of vhost-server
        pid = int(out_r.readline().strip())
        print("Vhost-server pid:", pid)

        # check that the server has lost the parent process
        __wait_message(err_r, 'Server started')
        __wait_message(err_r, 'Parent process exit.')

        # check that stat request works
        for _ in range(3):
            os.kill(pid, signal.SIGUSR1)
            s = __read_json(out_r)
            assert s['elapsed_ms'] != 0

        # wait vhost-server to shut down after a timeout
        __wait_message(err_r, 'Wait for timeout')
        __wait_message(err_r, 'Server has been stopped')

        assert not os.path.exists(socket_path)
