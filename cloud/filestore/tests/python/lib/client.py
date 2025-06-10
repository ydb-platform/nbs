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


class FilestoreCliClient:
    def __init__(
        self,
        binary_path,
        port,
        vhost_port=None,
        verbose=False,
        cwd=".",
        timeout=60,
        config_path=None,
        auth_token=None,
        check_exit_code=True,
        return_json=False,
        unix_socket=None,
    ):
        self.__binary_path = binary_path
        self.__port = port
        self.__vhost_port = vhost_port
        self.__verbose = verbose
        self.__cwd = cwd
        self.__timeout = timeout
        self.__config_path = config_path
        self.__env = {}
        self.__check_exit_code = check_exit_code
        self.__return_json = return_json
        self.__server_unix_socket_path = unix_socket
        if auth_token is not None:
            self.__env = {"IAM_TOKEN": auth_token}

    def create(
        self,
        fs,
        cloud,
        folder,
        blk_size=4096,
        blk_count=100 * 1024 * 1024 * 1024,
        return_stdout=True,
        verbose=False,
    ):
        cmd = [
            self.__binary_path, "create",
            "--filesystem", fs,
            "--cloud", cloud,
            "--folder", folder,
            "--block-size", str(blk_size),
            "--blocks-count", str(blk_count),
        ] + self.__cmd_opts()

        if verbose:
            cmd += ["--verbose", "trace"]

        logger.info("creating filestore: " + " ".join(cmd))
        result = common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)
        if return_stdout:
            return result.stdout

        return result

    def destroy(self, fs):
        cmd = [
            self.__binary_path, "destroy",
            "--filesystem", fs,
        ] + self.__cmd_opts()

        logger.info("destroying filestore: " + " ".join(cmd))
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def mount(self, fs, path, mount_seqno=0, readonly=False):
        cmd = [
            self.__binary_path, "mount",
            "--filesystem", fs,
            "--mount-path", path,
            "--mount-seqno", str(mount_seqno)
        ] + self.__cmd_opts()

        if readonly:
            cmd.append("--mount-readonly")

        logger.info("mounting filestore: " + " ".join(cmd))
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

    def resize(self, fs, blk_count, force=False, shard_count=None):
        cmd = [
            self.__binary_path, "resize",
            "--filesystem", fs,
            "--blocks-count", str(blk_count),
        ] + self.__cmd_opts()

        if force:
            cmd.append("--force")

        if shard_count is not None:
            cmd += ["--shard-count", str(shard_count)]

        logger.info("resizing filestore: " + " ".join(cmd))
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def list_filestores(self):
        cmd = [
            self.__binary_path, "listfilestores",
        ] + self.__cmd_opts()

        names = common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout.decode().splitlines()
        return sorted(names)

    def start_endpoint(self, fs, socket, mount_seqno, readonly, persistent=False, client_id=""):
        cmd = [
            self.__binary_path, "startendpoint",
            "--filesystem", fs,
            "--socket-path", socket,
            "--mount-seqno", str(mount_seqno),
            "--client-id", client_id,
        ] + self.__cmd_opts(vhost=True)

        if readonly:
            cmd.append("--mount-readonly")

        if persistent:
            cmd.append("--persistent")

        logger.info("starting endpoint: " + " ".join(cmd))
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)

    def stop_endpoint(self, socket):
        cmd = [
            self.__binary_path, "stopendpoint",
            "--socket-path", socket,
        ] + self.__cmd_opts(vhost=True)

        logger.info("stopping endpoint: " + " ".join(cmd))
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)

    def list_endpoints(self):
        cmd = [
            self.__binary_path, "listendpoints",
        ] + self.__cmd_opts(vhost=True)

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)

    def kick_endpoint(self, keyring_id):
        cmd = [
            self.__binary_path, "kickendpoint",
            "--keyring-id", str(keyring_id),
        ] + self.__cmd_opts(vhost=True)

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)

    def create_session(self, fs, session_id, client_id):
        cmd = [
            self.__binary_path, "createsession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def reset_session(self, fs, session_id, client_id, session_state):
        cmd = [
            self.__binary_path, "resetsession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
            "--session-state", base64.b64encode(session_state).decode("utf-8"),
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def destroy_session(self, fs, session_id, client_id):
        cmd = [
            self.__binary_path, "destroysession",
            "--filesystem", fs,
            "--session-id", session_id,
            "--client-id", client_id,
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def stat(self, fs, path):
        cmd = [
            self.__binary_path, "stat",
            "--filesystem", fs,
            "--path", path,
            "--json",
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def find_garbage(
            self,
            fs,
            shards,
            page_size,
            find_in_shards=True,
            find_in_leader=False):
        shard_params = []
        for shard in shards:
            shard_params += ["--shard", shard]
        shard_params += ["--find-in-shards", str(int(find_in_shards))]
        shard_params += ["--find-in-leader", str(int(find_in_leader))]
        cmd = [
            self.__binary_path, "findgarbage",
            "--filesystem", fs,
            "--page-size", str(page_size),
        ] + shard_params + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def set_node_attr(self, fs, node_id, *argv):
        list_args = [str(x) for x in argv]
        cmd = [
            self.__binary_path, "setnodeattr",
            "--filesystem", fs,
            "--node-id", str(node_id),
        ] + list_args + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def forced_compaction(self, fs):
        cmd = [
            self.__binary_path, "forcedcompaction",
            "--filesystem", fs,
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def find(self, fs, depth, glob=None):
        cmd = [
            self.__binary_path, "find",
            "--filesystem", fs,
            "--depth", str(depth),
        ] + (["--glob", glob] if glob is not None else []) + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def diff(self, fs, other_fs):
        cmd = [
            self.__binary_path, "diff",
            "--filesystem", fs,
            "--other-filesystem", other_fs,
            "--diff-content",
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

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

        res = common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code)
        os.unlink(request_file.name)
        return res.stdout

    def get_storage_service_config(self, fs_id=None):
        req = {"FileSystemId": "" if fs_id is None else fs_id}

        resp = self.execute_action("getstorageconfig", req)

        return json.loads(resp)

    def mv(self, fs, src_path, dst_path):
        cmd = [
            self.__binary_path, "mv",
            "--filesystem", fs,
            "--src-path", src_path,
            "--dst-path", dst_path,
        ] + self.__cmd_opts()

        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    def __cmd_opts(self, vhost=False):
        opts = [
            "--server-address", "localhost",
            "--server-port", str(self.__port if not vhost else self.__vhost_port),
        ]
        if self.__config_path is not None:
            opts = ["--config", self.__config_path]
        if self.__verbose:
            opts += ["--verbose", "trace"]
        if self.__return_json:
            opts += ["--json"]
        if self.__server_unix_socket_path is not None:
            opts += ["--server-unix-socket-path", self.__server_unix_socket_path]
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
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    @standard_command("mkdir")
    def mkdir(self, cmd):
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    @standard_command("write")
    def write(self, cmd):
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    @standard_command("read")
    def read(self, cmd):
        return common.execute(cmd).stdout

    @standard_command("touch")
    def touch(self, cmd):
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    @standard_command("rm")
    def rm(self, cmd):
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout

    @standard_command("ln")
    def ln(self, cmd):
        return common.execute(cmd, env=self.__env, check_exit_code=self.__check_exit_code).stdout


def create_endpoint(
        client,
        filesystem,
        socket_path,
        socket_prefix,
        endpoint_storage_dir,
        mount_seqno=0,
        readonly=False,
        client_id=""):

    _uid = str(uuid.uuid4())

    socket = os.path.join(
        socket_path,
        socket_prefix + "." + _uid)
    socket = os.path.abspath(socket)

    persistent = (endpoint_storage_dir is not None)
    client.start_endpoint(
        filesystem,
        socket,
        mount_seqno,
        readonly,
        persistent=persistent,
        client_id=client_id)

    return socket


def make_socket_generator(
        filestore_port,
        vhost_port,
        filesystem="nfs_test",
        socket_path="/tmp",
        socket_prefix="test.vhost",
        endpoint_storage_dir=None):

    client_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")

    filestore_client = FilestoreCliClient(
        client_path,
        filestore_port,
        vhost_port=vhost_port,
        verbose=True,
        cwd=common.output_path())

    def socket_generator(mount_seqno=0, readonly=False):
        return create_endpoint(
            filestore_client,
            filesystem,
            socket_path,
            socket_prefix,
            endpoint_storage_dir,
            mount_seqno,
            readonly)

    return socket_generator
