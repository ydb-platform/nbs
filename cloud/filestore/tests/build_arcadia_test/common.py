import string
import time
from contextlib import contextmanager
from typing import Dict

from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper
from library.python import resource

LOG_PATH = '/root/log.txt'
LOG_LOCAL_PATH = 'log.txt'
MOUNT_PATH = '/test'

DEVICE_NAME = 'nfs'
TEST_FS_SIZE = 500  # GB


class Error(Exception):
    pass


class ResourceExhaustedError(Exception):
    pass


def safe_run_ssh_command(
    instance_ip: str,
    dry_run: bool,
    cmd: str,
    logger,
    module_factories,
    ssh_key_path: str | None = None
):
    with module_factories.make_ssh_client(dry_run, instance_ip, ssh_key_path) as ssh:
        _, stdout, stderr = ssh.exec_command(cmd)
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            logger.error(f'Failed to run command: {cmd}; with exit code: {exit_code}:\n'
                         f'stderr: {"".join(stderr.readlines())}\n'
                         f'stdout: {"".join(stdout.readlines())}')
            raise Error('Failed to run command')

        out = stdout.read().decode("utf-8")
        logger.debug(f'Command: {cmd}\n'
                     f'stderr: {"".join(stderr.readlines())}\n'
                     f'stdout: {out}')

        return out


def run(
    instance_ip: str,
    dry_run: bool,
    script_name: str,
    script_path: str,
    vars: Dict[str, str],
    logger,
    module_factories,
    ssh_key_path: str | None = None,
):
    logger.info(f'Running {script_name} on instance')

    template = string.Template(resource.find(f'{script_name}').decode('utf8'))
    script = template.substitute(**vars)

    with module_factories.make_sftp_client(dry_run, instance_ip, ssh_key_path) as sftp:
        file = sftp.file(script_path, 'w')
        file.write(script)
        file.flush()
        file.close()
        sftp.chmod(script_path, 0o755)

    channel = module_factories.make_ssh_channel(dry_run, instance_ip, ssh_key_path)
    channel.exec_command(f'{script_path} > {LOG_PATH} 2>&1')

    while True:
        out = safe_run_ssh_command(instance_ip, dry_run, f'ps aux | grep {script_name}',
                                   logger, module_factories, ssh_key_path)
        lines = out.count('\n')
        if dry_run or lines == 2:
            logger.info('Script finished')
            break
        logger.info('Script is still running:\n'
                    f'{out}'
                    'sleeping for 1 minute')
        time.sleep(60)


def fetch_file_from_vm(
    dry_run: bool,
    instance_ip: str,
    source: str,
    destination: str,
    module_factories,
    ssh_key_path: str | None = None
):
    with module_factories.make_sftp_client(dry_run, instance_ip, ssh_key_path) as sftp:
        sftp.get(source, destination)


@contextmanager
def create_instance(
    ycp: YcpWrapper,
    cores: int,
    memory: int,
    compute_node: str,
    image_name: str,
    image_folder_id: str,
    platform_ids: list[str],
    description: str,
    logger
) -> Ycp.Instance:
    created = False
    for platform_id in platform_ids:
        if created:
            break

        try:
            with ycp.create_instance(
                    cores=cores,
                    memory=memory,
                    compute_node=compute_node,
                    image_name=image_name,
                    image_folder_id=image_folder_id,
                    platform_id=platform_id,
                    description=description) as instance:
                created = True
                yield instance
        except Exception as e:
            if created:
                raise e
            else:
                logger.info(f'Error: {e}')
                logger.info(f'Cannot create VM on platform {platform_id}')

    if not created:
        raise ResourceExhaustedError(f'Cannot create VM on any platform'
                                     f' from {platform_ids}')


def create_fs_over_nbs_disk(instance_ip: str, dry_run: bool, logger, module_factories, ssh_key_path: str | None = None):
    logger.info('Creating fs over nbs disk')
    with module_factories.make_sftp_client(dry_run, instance_ip, ssh_key_path) as sftp:
        sftp.mkdir(MOUNT_PATH)
        safe_run_ssh_command(
            instance_ip,
            dry_run,
            f'mkfs.ext4 /dev/vdb && mount /dev/vdb {MOUNT_PATH}',
            logger,
            module_factories,
            ssh_key_path)


def mount_fs(instance_ip: str, dry_run: bool, logger, module_factories, ssh_key_path: str | None = None):
    logger.info('Mounting fs')
    with module_factories.make_sftp_client(dry_run, instance_ip, ssh_key_path) as sftp:
        sftp.mkdir(MOUNT_PATH)
        safe_run_ssh_command(
            instance_ip,
            dry_run,
            f'mount -t virtiofs {DEVICE_NAME} {MOUNT_PATH}',
            logger,
            module_factories,
            ssh_key_path)


@contextmanager
def create_disk(
    ycp: YcpWrapper,
    disk_size: int,
    type_id: str,
    logger
) -> Ycp.Disk:
    created = False
    try:
        with ycp.create_disk(size=disk_size, type_id=type_id) as disk:
            created = True
            yield disk
    except Exception as e:
        if created:
            raise e
        else:
            logger.info(f'Error: {e}')
            raise ResourceExhaustedError('Cannot create disk')


@contextmanager
def create_fs(
    ycp: YcpWrapper,
    fs_size: int,
    type_id: str,
    logger
) -> Ycp.Filesystem:
    created = False
    try:
        with ycp.create_fs(size=fs_size, type_id=type_id) as fs:
            created = True
            yield fs
    except Exception as e:
        if created:
            raise e
        else:
            logger.info(f'Error: {e}')
            raise ResourceExhaustedError('Cannot create file system')


def find_fs(
    ycp: YcpWrapper,
    fs_id: str,
    logger
) -> Ycp.Filesystem:
    try:
        fss = ycp.list_filesystems()
        for fs in fss:
            if fs.id == fs_id:
                return fs
    except Exception as e:
        logger.info(f'Error occurs while finding fs ({fs_id}) {e}')
        raise Error('Cannot find fs')
