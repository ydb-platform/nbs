import argparse
import io
import json
import logging
import sys
import time

from subprocess import Popen, PIPE, DEVNULL, run


FS_UNK = 'unknown'
FS_EXT4 = 'ext4'
FS_XFS = 'xfs'


def prepare_logging(args):
    log_level = logging.ERROR

    if args.silent:
        log_level = logging.INFO
    elif args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))

    root = logging.getLogger()
    root.setLevel(log_level)

    formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] %(message)s")

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    fh = logging.FileHandler(filename=f'cleanup-fs.{args.disk_id}.log', encoding='utf-8')
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    root.addHandler(fh)


def get_dev_size(path):
    p = run(['blockdev', '--getsize64', path], stdout=PIPE, check=True, text=True)
    logging.debug(f'blockdev --getsize64 {path}: {p.stdout}')
    return int(p.stdout)


def mount_volume(disk_id, dev_path):
    logging.info(f'mount volume {args.disk_id} to {dev_path} ...')

    socket_path = f"/var/tmp/{disk_id}.nbd.socket"

    nbd = Popen([
        'blockstore-nbd',
        '--verbose', 'error',
        '--connect-device', dev_path,
        '--disk-id', disk_id,
        '--mount-mode', 'local',
        '--throttling-disabled', '1',
        '--device-mode', 'endpoint',
        '--listen-path', socket_path,
        '--access-mode', 'rw'
    ], stderr=PIPE, text=True)

    def check_process():
        logging.info('check process ...')
        if nbd.poll() is not None:
            logging.warning(f'something went wrong: {nbd.returncode} ...')
            raise Exception(f"can't mount volume {args.disk_id}: {nbd.stderr.read()}")
        logging.info('OK')

    while True:
        s = get_dev_size(dev_path)
        logging.info(f'device size: {s}')
        if s != 0:
            break
        logging.info('wait for device...')
        time.sleep(1)

        check_process()

    time.sleep(1)
    check_process()

    return nbd


def cleanup_ext4(disk_id, partition, target_path):
    logging.info(f'cleanup ext4 {target_path} ...')

    dump_path = f"dumpe2fs.{disk_id}.{partition}.txt"
    with open(dump_path, 'w') as f:
        run(['./dumpe2fs', target_path], check=True, stdout=f)

    logging.info('e2fsck -E discard ...')
    code = run(['./e2fsck', '-fy', '-E', 'discard', target_path], check=False).returncode
    if code == 1:
        logging.warn('encountered some errors during the first fsck stage')
    elif code != 0:
        raise Exception('unexpected e2fsck exit code %s' % code)

    logging.info('cleanup meta ...')

    with open(f'cleanup-meta.{disk_id}.{partition}.log', 'w') as f:
        run([
            './cleanup-ext4-meta',
            '--clean-all', '--verbose',
            '--dumpe2fs-output-file', dump_path,
            '--filesystem-path', target_path
        ], check=True, stdout=f)

    logging.info('e2fsck ...')
    run(['./e2fsck', '-fvn', target_path], check=True)


def cleanup_xfs(disk_id, partition, target_path):
    logging.info(f'cleanup xfs {target_path} ...')

    def xfs_db(tag):
        dump_path = f"xfs_db.{disk_id}.{partition}.{tag}.txt"
        with open(dump_path, 'w') as f:
            run(['./xfs_db', '-c', 'sb', '-c', 'p', target_path], check=True, stdout=f)

    xfs_db('before')

    with open(f'cleanup-xfs.{disk_id}.{partition}.log', 'w') as f:
        run([
            './cleanup-xfs',
            '--device-path', target_path,
            '--xfs_db-path', './xfs_db',
            '--verbose',
        ], check=True, stdout=f)

    xfs_db('after')


def get_partitions(device):
    sfdisk = run(
        ['sfdisk', '-J', device],
        stdout=PIPE,
        text=True,
        check=False)

    logging.debug(f"sfdisk: {sfdisk.returncode} | {sfdisk.stdout} ...")

    if sfdisk.returncode != 0:
        size = get_dev_size(device)
        return [(0, device, size, size)]

    x = json.loads(sfdisk.stdout)["partitiontable"]

    if x["unit"] != "sectors":
        raise Exception(f'unexpected: {x["unit"]} != "sectors"')

    if device != x["device"]:
        raise Exception(f'unexpected: {x["device"]} != "{device}"')

    partitions = []
    for i, p in enumerate(x["partitions"]):
        num = i + 1
        if f"{device}p{num}" != p["node"]:
            raise Exception(f'unexpected: "{device}p{num}" != {p["node"]}')

        start = int(p["start"]) * 512
        size = int(p["size"]) * 512
        # num, path, end, size
        partitions.append((num, p["node"], start + size, size))

    return partitions


def try_detect_fs(path):
    logging.debug(f"try ext4 on {path} ...")

    p = run(['./dumpe2fs', '-h', path], check=False, stdout=DEVNULL)
    if p.returncode == 0:
        return FS_EXT4

    logging.debug(f"try xfs on {path} ...")
    p = run(['./xfs_db', '-c', 'sb', '-c', '-p', path], check=False, stdout=DEVNULL)
    if p.returncode == 0:
        return FS_XFS

    return FS_UNK


def describe_volume(disk_id, secure):
    args = [
        'blockstore-client',
        'describevolume', '--disk-id', disk_id,
        '--verbose', 'error',
    ]

    if secure:
        args += ["--secure-port", "9768"]

    p = run(args, stdout=PIPE, check=True, text=True)

    logging.debug(f'[describe_volume] {disk_id}: {p.stdout}')

    descr = {}

    s = io.StringIO(p.stdout)

    for line in s:
        ss = line.strip().split(': ')

        if len(ss) != 2:
            continue

        k, v = ss

        if k in ['BlockSize', 'BlocksCount']:
            descr[k] = int(v)

    return descr


# use blockstore-client backupvolume ...
def backup_volume(disk_id, secure):
    backup_disk_id = disk_id + '.cleanup.bkp'

    logging.debug(f'[backup_volume] backup {disk_id} to {backup_disk_id} ...')

    args = [
        'blockstore-client', 'backupvolume',
        '--verbose', 'error',
        '--disk-id', disk_id,
        '--backup-disk-id', backup_disk_id,
        '--storage-media-kind', 'ssd',
    ]

    if secure:
        args += ["--secure-port", "9768"]

    run(args, check=True)


def create_backup_volume(disk_id, secure):
    d = describe_volume(disk_id, secure)

    if not d:
        raise Exception(f"failed to describe volume {disk_id}")

    logging.debug(f'[create_backup_volume] {disk_id}: {d}')

    backup_disk_id = disk_id + '.cleanup.bkp'

    args = [
        'blockstore-client', 'createvolume',
        '--verbose', 'error',
        '--disk-id', backup_disk_id,
        '--storage-media-kind', 'ssd',
        '--blocks-count', str(d['BlocksCount']),
        '--block-size', str(d['BlockSize']),
    ]

    if secure:
        args += ["--secure-port", "9768"]

    run(args, check=True)

    return backup_disk_id


# use copy_dev ...
def copy_dev(disk_id, src_dev, secure):
    bkp_dev = '/dev/nbd5'
    backup_disk_id = create_backup_volume(disk_id, secure)

    bkp = mount_volume(backup_disk_id, bkp_dev)

    try:
        logging.debug(f'[copy_dev] backup {disk_id} to {backup_disk_id} ...')
        run(['./copy_dev', '-s', src_dev, '-d', bkp_dev, '-v'], check=True)
    finally:
        logging.info(f'unmount volume {backup_disk_id} from {bkp_dev} ...')
        bkp.terminate()
        bkp.wait()
        logging.info('done')


def main(args):
    logging.info(f'cleanup disk {args.disk_id} ...')

    if args.backup_volume == "none":
        logging.info('skip backup')

    if args.backup_volume == "nbs":
        backup_volume(args.disk_id, not args.no_auth)

    nbd = mount_volume(args.disk_id, args.device)

    try:
        if args.backup_volume == "copy_dev":
            copy_dev(args.disk_id, args.device, not args.no_auth)

        partitions = []

        if args.fs_type == 'auto':  # ignore args.partition
            partitions = get_partitions(args.device)
        else:
            target_path = args.device
            if args.partition != '0':
                target_path += 'p' + args.partition
            partitions = [(int(args.partition), target_path, 1024**3, get_dev_size(target_path))]

        logging.debug(f"partitions: {partitions}")

        for num, path, end, size in partitions:
            logging.info(f"cleanup partition {num} {path} {end} {size} ...")
            if end < 1024**3:  # 1GiB already clean
                logging.info(f"skip partition {path}")
                continue

            fs = args.fs_type

            if args.fs_type == 'auto':
                fs = try_detect_fs(path)

            logging.debug(f"filesystem for {path} : {fs}")

            if fs == FS_UNK:
                print(f"unknown filesystem for {path}. skip")
                continue

            if fs == FS_EXT4:
                cleanup_ext4(args.disk_id, num, path)
            elif fs == FS_XFS:
                cleanup_xfs(args.disk_id, num, path)
            else:
                assert False
    finally:
        logging.info(f'unmount volume {args.disk_id} ...')
        nbd.terminate()
        nbd.wait()
        logging.info('done')

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--disk-id", type=str, required=True)

    parser.add_argument(
        "--fs-type",
        type=str,
        choices=[FS_EXT4, FS_XFS, "auto"],
        default="auto"
    )
    parser.add_argument("--partition", type=str, default="0")
    parser.add_argument("--device", type=str, default="/dev/nbd0")
    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    parser.add_argument("--no-auth", action="store_true", default=False)
    parser.add_argument("--backup-volume", choices=["none", "copy_dev", "nbs"], default="none")

    args = parser.parse_args()

    prepare_logging(args)

    r = None

    try:
        r = main(args)

        if r == 0:
            print("SUCCESS")
            sys.exit(0)
    except Exception as e:
        logging.warning("[main] error: {}".format(e))
        r = 1

    print("FAILED")
    sys.exit(r)
