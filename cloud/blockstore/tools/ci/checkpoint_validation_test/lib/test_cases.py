from dataclasses import dataclass

from .errors import Error

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.ycp import Ycp


@dataclass
class TestConfig:
    instance: Ycp.Instance
    disk: Ycp.Disk
    checkpoint_id: str
    config_json: str


def find_instance(ycp: Ycp, args):
    instances = ycp.list_instances(folder_id=args.folder_id)
    if args.dry_run:
        return instances[0]
    for instance in instances:
        if instance.name == args.vm_name:
            return instance


def find_disk(ycp: Ycp, args):
    disks = ycp.list_disks(folder_id=args.folder_id)
    if args.dry_run:
        return disks[0]
    for disk in disks:
        if disk.name == args.disk_name:
            return disk


def get_test_config(
    module_factories: common.ModuleFactories,
    ycp: Ycp,
    helpers,
    args,
    logger
) -> TestConfig:
    logger.info('Generating test config')
    instance = find_instance(ycp, args)
    if instance is None:
        raise Error(f'No instance with name=<{args.vm_name}> found')

    disk = find_disk(ycp, args)
    if disk is None:
        raise Error(f'No disk with name=<{args.disk_name}> found')

    with module_factories.make_sftp_client(args.dry_run, instance.ip, ssh_key_path=args.ssh_key_path) as sftp:
        file = sftp.file(args.file_name)
        cfg = "".join(file.readlines())

    return TestConfig(
        instance=instance,
        disk=disk,
        checkpoint_id=f'tmp-checkpoint-{helpers.generate_id()}',
        config_json=cfg)
