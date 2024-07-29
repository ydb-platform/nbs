import base64
import json
import logging
import os
import signal
import tempfile
import uuid

import yatest.common as common

from cloud.filestore.tests.python.lib.common import daemon_log_files, wait_for

from contrib.ydb.tests.library.harness.daemon import Daemon

logger = logging.getLogger(__name__)

KEYRING_FILE_NAME = "vhost-endpoint-keyring-name.txt"


class NfsCliClient:
    def __init__(self, binary_path, port, vhost_port=None, verbose=False, cwd=".", timeout=60):
        self.__binary_path = binary_path
        self.__port = port
        self.__vhost_port = vhost_port
        self.__verbose = verbose
        self.__cwd = cwd
        self.__timeout = timeout

    def create(self, fs, cloud, folder, blk_size=4096, blk_count=100 * 1024 * 1024 * 1024):
        cmd = [
            self.__binary_path, "create",
            "--filesystem", fs,
            "--cloud", cloud,
            "--folder", folder,
            "--block-size", str(blk_size),
            "--blocks-count", str(blk_count)
        ] + self.__cmd_opts()

        logger.info("creating nfs: " + " ".join(cmd))
        return common.execute(cmd).stdout

    def destroy(self, fs):
        cmd = [
            self.__binary_path, "destroy",
            "--filesystem", fs,
        ] + self.__cmd_opts()

        logger.info("destroying nfs: " + " ".join(cmd))
        return common.execute(cmd).stdout

    def mount(self, fs, path, mount_seqno=0, readonly=False):
        cmd = [
            self.__binary_path, "mount",
            "--filesystem", fs,
            "--mount-path", path,
            "--mount-seqno", str(mount_seqno)
        ] + self.__cmd_opts()

        if readonly:
            cmd.append("--mount-readonly")

        logger.info("mounting nfs: " + " ".join(cmd))
        mount = Daemon(
            cmd,
            cwd=self.__cwd,
            timeout=self.__timeout,
            **daemon_log_files(prefix="nfs-mnt-%s" % fs, cwd=self.__cwd))
        mount.start()

        pid = mount.daemon.process.pid
        logger.info("mount for " + fs + " pid: " + str(pid))

        if not wait_for(lambda: os.path.ismount(path), self.__timeout):
            os.kill(pid, signal.SIGTERM)
            raise RuntimeError("failed to mount {} at {} in {}".format(
                fs, path, self.__timeout))

        return pid

    def list_filestores(self):
        cmd = [
            self.__binary_path, "listfilestores",
        ] + self.__cmd_opts()

        names = common.execute(cmd).stdout.decode().splitlines()
        return sorted(names)

    def start_endpoint(self, fs, socket, mount_seqno, readonly, persistent=False):
        cmd = [
            self.__binary_path, "startendpoint",
            "--filesystem", fs,
            "--socket-path", socket,
            "--mount-seqno", str(mount_seqno),
        ] + self.__cmd_opts(vhost=True)

        if readonly:
            cmd.append("--mount-readonly")

        if persistent:
            cmd.append("--persistent")

        logger.info("starting endpoint: " + " ".join(cmd))
        return common.execute(cmd)

    def stop_endpoint(self, socket):
        cmd = [
            self.__binary_path, "stopendpoint",
            "--socket-path", socket,
        ] + self.__cmd_opts(vhost=True)

        logger.info("stopping endpoint: " + " ".join(cmd))
        return common.execute(cmd)

    def list_endpoints(self):
        cmd = [
            self.__binary_path, "listendpoints",
        ] + self.__cmd_opts(vhost=True)

        return common.execute(cmd)

    def kick_endpoint(self, keyring_id):
        cmd = [
            self.__binary_path, "kickendpoint",
            "--keyring-id", str(keyring_id),
        ] + self.__cmd_opts(vhost=True)

        return common.execute(cmd)

    def create_session(self, fs, session_id, client_id):
        cmd = [
            self.__binary_path, "createsession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
        ] + self.__cmd_opts()

        return common.execute(cmd).stdout

    def reset_session(self, fs, session_id, client_id, session_state):
        cmd = [
            self.__binary_path, "resetsession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
            "--session-state", base64.b64encode(session_state).decode("utf-8"),
        ] + self.__cmd_opts()

        return common.execute(cmd).stdout

    def destroy_session(self, fs, session_id, client_id):
        cmd = [
            self.__binary_path, "destroysession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
        ] + self.__cmd_opts()

        return common.execute(cmd).stdout

    def stat(self, fs, path):
        cmd = [
            self.__binary_path, "stat",
            "--filesystem", fs,
            "--path", path,
            "--json",
        ] + self.__cmd_opts()

        return common.execute(cmd).stdout

    def find_garbage(self, fs, shards):
        shard_params = []
        for shard in shards:
            shard_params += ["--shard", shard]
        cmd = [
            self.__binary_path, "findgarbage",
            "--filesystem", fs,
        ] + shard_params + self.__cmd_opts()

        return common.execute(cmd).stdout

    def set_node_attr(self, fs, node_id, *argv):
        list_args = [str(x) for x in argv]
        cmd = [
            self.__binary_path, "setnodeattr",
            "--filesystem", fs,
            "--node-id", str(node_id),
        ] + list_args + self.__cmd_opts()

        return common.execute(cmd).stdout

    def execute_action(self, action, request):
        request_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        json.dump(request, request_file)
        request_file.close()
        cmd = [
            self.__binary_path, "executeaction",
            "--action", action,
            "--input-file", request_file.name,
        ] + self.__cmd_opts()
        print(cmd)

        res = common.execute(cmd)
        os.unlink(request_file.name)
        return res.stdout

    def get_storage_service_config(self, fs_id=None):
        req = {"FileSystemId": "" if fs_id is None else fs_id}

        resp = self.execute_action("getstorageconfig", req)

        return json.loads(resp)

    def __cmd_opts(self, vhost=False):
        opts = [
            "--server-address", "localhost",
            "--server-port", str(self.__port if not vhost else self.__vhost_port),
        ]

        if self.__verbose:
            opts += ["--verbose", "trace"]

        return opts

    def standard_command(input_arg):
        def the_real_decorator(function):
            def wrapper(self, fs, path, *custom_opts):
                cmd = [
                    self.__binary_path, input_arg,
                    "--filesystem", fs,
                    "--path", path,
                ] + self.__cmd_opts() + [*custom_opts]
                logger.info("executing" + input_arg + ": " + " ".join(cmd))
                return function(self, cmd)

            return wrapper

        return the_real_decorator

    @standard_command("ls")
    def ls(self, cmd):
        return common.execute(cmd).stdout

    @standard_command("mkdir")
    def mkdir(self, cmd):
        return common.execute(cmd).stdout

    @standard_command("write")
    def write(self, cmd):
        return common.execute(cmd).stdout

    @standard_command("touch")
    def touch(self, cmd):
        return common.execute(cmd).stdout

    @standard_command("rm")
    def rm(self, cmd):
        return common.execute(cmd).stdout


def create_endpoint(client, filesystem, socket_path, socket_prefix, endpoint_storage_dir, mount_seqno=0, readonly=False):
    _uid = str(uuid.uuid4())

    socket = os.path.join(
        socket_path,
        socket_prefix + "." + _uid)
    socket = os.path.abspath(socket)

    persistent = (endpoint_storage_dir is not None)
    client.start_endpoint(filesystem, socket, mount_seqno, readonly, persistent=persistent)

    return socket


def make_socket_generator(
        nfs_port,
        vhost_port,
        filesystem="nfs_test",
        socket_path="/tmp",
        socket_prefix="test.vhost",
        endpoint_storage_dir=None):

    client_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")

    nfs_client = NfsCliClient(
        client_path,
        nfs_port,
        vhost_port=vhost_port,
        verbose=True,
        cwd=common.output_path())

    def socket_generator(mount_seqno=0, readonly=False):
        return create_endpoint(
            nfs_client,
            filesystem,
            socket_path,
            socket_prefix,
            endpoint_storage_dir,
            mount_seqno,
            readonly)

    return socket_generator
