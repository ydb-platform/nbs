import argparse
import io
import json
import logging
import os
import sys
import time

from subprocess import Popen, PIPE, run


def prepare_logging(args):
    log_level = logging.ERROR

    if args.silent:
        log_level = logging.INFO
    elif args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))

    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


def invoke_blockdev(path, arg):
    p = run(
        ['blockdev', '--' + arg, path],
        stdout=PIPE,
        stderr=sys.stdout,
        check=True,
        text=True)
    return int(p.stdout)


def get_dev_size(path):
    return invoke_blockdev(path, 'getsize64')


def parse_volume_info(stream):
    info = {}
    for line in stream:
        if line.startswith('BlockSize: '):
            info['bs'] = int(line.split(':')[1].strip())
            continue
        if line.startswith('BlocksCount: '):
            info['bc'] = int(line.split(':')[1].strip())
            continue

    return info


def mount_volume(disk_id, path, iam_token_file, mount_delay):
    logging.info(f'mount volume {disk_id} to {path} ...')

    args = [
        'blockstore-nbd',
        '--verbose', 'error',
        '--connect-device', path,
        '--disk-id', disk_id,
        '--mount-mode', 'remote',
        '--throttling-disabled', '1',
        '--device-mode', 'endpoint',
        '--listen-path', f"/var/tmp/{disk_id}.nbd.socket",
        '--access-mode', 'ro'
    ]

    if iam_token_file:
        args += [
            "--secure-port", "9768",
            "--iam-token-file", iam_token_file
        ]

    nbd = Popen(args, stderr=PIPE, text=True)

    def check_process():
        logging.debug(f'wait for device {mount_delay} sec ...')
        time.sleep(mount_delay)

        logging.debug('check process ...')
        if nbd.poll() is not None:
            logging.warning(f'something went wrong: {nbd.returncode} ...')
            raise Exception(f"can't mount volume {disk_id}: {nbd.stderr.read()}")
        logging.debug('OK')

    while True:
        s = get_dev_size(path)
        logging.info(f'device size: {s}')
        if s != 0:
            break

        check_process()

    check_process()

    return nbd


class MountedVolume:

    def __init__(self, disk_id, path, iam_token_file, delay_ms):
        self.__disk_id = disk_id
        self.__path = path
        self.__process = None
        self.__iam_token_file = iam_token_file
        self.__delay = delay_ms / 1000.0

    @property
    def path(self):
        return self.__path

    def __enter__(self):
        self.__process = mount_volume(
            self.__disk_id,
            self.__path,
            self.__iam_token_file,
            self.__delay)

        return self

    def __exit__(self, type, value, traceback):
        if self.__process is not None:
            logging.info(f'unmount volume {self.__disk_id} from {self.__path} ...')
            self.__process.terminate()
            self.__process.wait()

        return False


class LoopDev:

    def __init__(self, dev_path, offset):
        self.__orig_path = dev_path
        self.__path = None
        self.__offset = offset

    @property
    def original_path(self):
        return self.__orig_path

    @property
    def path(self):
        return self.__path

    @property
    def offset(self):
        return self.__offset

    def __enter__(self):
        logging.info(f'setup loop device over {self.__orig_path}')

        cmd = [
            'losetup',
            '-o', str(self.__offset),
            '-vrf',
            '--show',
            self.__orig_path
        ]

        p = run(
            cmd,
            stdout=PIPE,
            stderr=sys.stdout,
            check=True,
            text=True
        )

        self.__path = p.stdout.strip()
        logging.info(f'loop device over {self.__orig_path}: {self.__path}')

        assert self.__path is not None

        return self

    def __exit__(self, type, value, traceback):
        if self.__path is not None:
            logging.info(f'remove loop device: {self.__path}')
            run(
                ['losetup', '-v', '-d', self.__path],
                stdout=PIPE,
                stderr=sys.stdout,
                check=True,
                text=True
            )
            time.sleep(5)

        return False


class NbsCli:

    def __init__(self, iam_token_file):
        self.__iam_token_file = iam_token_file

    def execute_action(self, action_name, input_bytes):
        logging.info(f'execute action {action_name} with {input_bytes}')

        args = [
            'blockstore-client', 'executeaction',
            '--action', action_name,
            '--verbose', 'error',
            '--input-bytes', json.dumps(input_bytes, separators=(',', ':')),
        ]

        if self.__iam_token_file:
            args += [
                "--secure-port", "9768",
                "--iam-token-file", self.__iam_token_file
            ]

        run(args, check=True, text=True, stdout=sys.stdout, stderr=sys.stdout)

    def describe_volume(self, disk_id):
        logging.info(f'describevolume {disk_id} ...')

        args = [
            'blockstore-client', 'describevolume',
            '--disk-id', disk_id,
            '--verbose', 'error'
        ]

        if self.__iam_token_file:
            args += [
                "--secure-port", "9768",
                "--iam-token-file", self.__iam_token_file
            ]

        p = run(args, check=True, text=True, stdout=PIPE, stderr=sys.stdout)

        s = io.StringIO(p.stdout)
        return parse_volume_info(s)

    def update_used_blocks(self, disk_id, blocks, max_batch_size, used=True):
        logging.info(f'install used blocks for {disk_id} ...')

        if max_batch_size == 0:
            # TODO: remove after NBS-2948
            for x, y in blocks:
                self.execute_action('updateusedblocks', {
                    "DiskId": disk_id,
                    "StartIndex": int(x),
                    "BlocksCount": int(y - x),
                    "Used": used
                })
        else:
            while blocks:
                batch = blocks[:max_batch_size] if max_batch_size > 0 else blocks

                self.execute_action('updateusedblocks', {
                    "DiskId": disk_id,
                    "StartIndices": [int(x) for x, _ in batch],
                    "BlockCounts": [int(y - x) for x, y in batch],
                    "Used": used
                })

                blocks = blocks[max_batch_size:]

    def add_tags(self, disk_id, tags):
        self.execute_action('modifytags', {
            "DiskId": disk_id,
            "TagsToAdd": tags
        })

    def remove_tags(self, disk_id, tags):
        self.execute_action('modifytags', {
            "DiskId": disk_id,
            "TagsToRemove": tags
        })

    def enable_mask_unused_blocks(self, disk_id):
        self.add_tags(disk_id, ["mask-unused"])

    def disable_mask_unused_blocks(self, disk_id):
        self.remove_tags(disk_id, ["mask-unused"])

    def enable_read_only_mode(self, disk_id):
        self.add_tags(disk_id, ["read-only"])

    def disable_read_only_mode(self, disk_id):
        self.remove_tags(disk_id, ["read-only"])

    def enable_track_used_blocks(self, disk_id):
        self.add_tags(disk_id, ["track-used"])

    def disable_track_used_blocks(self, disk_id):
        self.remove_tags(disk_id, ["track-used"])


def dump_disk_info(path, bs, output_dir):
    logging.info(f'dump disk info to {output_dir} ...')

    run([
        "./dump_disk_info",
        "--block-size", str(bs),
        "--output-dir", output_dir,
        "--path", path,
        "-vvv"
    ], check=True, text=True, stderr=sys.stdout)


def generate_used_blocks(path):
    logging.info(f'generate used blocks from {path} ...')

    args = [
        "./gen_used_blocks_map",
        "--meta-file", path,
        "--format", "json",
        "-vvv"
    ]

    p = run(args, check=True, text=True, stdout=PIPE, stderr=sys.stdout)

    return json.loads(p.stdout)


def reset_masks(args):
    cli = NbsCli(args.iam_token_file)

    cli.disable_mask_unused_blocks(args.disk_id)
    cli.enable_track_used_blocks(args.disk_id)
    descr = cli.describe_volume(args.disk_id)

    blocks = []
    start_index = 0
    bc = descr['bc']
    while start_index < bc:
        n = 200000
        if start_index + n > bc:
            n = bc - start_index

        blocks.append((start_index, start_index + n))

        start_index += n

    cli.update_used_blocks(args.disk_id, blocks, args.max_batch_size, used=False)
    cli.disable_track_used_blocks(args.disk_id)

    return 0


def fix_meta(path, device_offset):
    meta = None
    with open(path) as f:
        meta = json.load(f)

    logging.info(f'original meta: {meta}')

    size = meta['size'] - device_offset
    ss = meta['sector_size']

    new_meta = {
        'block_size': meta['block_size'],
        'size': size,
        'sector_size': ss,
        'partitions': {
            "label": "dos",
            "device": "/dev/nbd0",
            "unit": "sectors",
            'partitions': [
                {
                    "node": "/dev/nbd0p0",
                    "start": device_offset // ss,
                    "size": size // ss,
                    "type": "83",
                    "fs": meta['fs']
                }
            ]
        }
    }

    logging.info(f'new meta: {new_meta}')

    with open(path, 'w') as f:
        json.dump(new_meta, f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--disk-id", type=str, required=True)

    parser.add_argument("--device", type=str, default='/dev/nbd0', help='mount point for disk')
    parser.add_argument("--output-dir", type=str, default='./dumps', help='output folder')
    parser.add_argument("--iam-token-file", type=str, help='IAM token file')
    parser.add_argument("--mask-unused", help="set 'mask-unused' tag", action='store_true', default=False)
    parser.add_argument("--always-track", help="alway set track-used flag", action='store_true', default=False)
    parser.add_argument("--reset-masks", help="remove used blocks mask from disk and 'mask-unused' 'track-used' tags", action='store_true', default=False)
    parser.add_argument("--max-batch-size", type=int, default=5000)
    parser.add_argument("--mount-delay-ms", type=int, default=100)
    parser.add_argument("--device-offset", type=int, default=None)

    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    args = parser.parse_args()

    prepare_logging(args)

    if args.reset_masks:
        return reset_masks(args)

    output_dir = os.path.join(args.output_dir, args.disk_id)

    cli = NbsCli(args.iam_token_file)

    descr = cli.describe_volume(args.disk_id)

    cli.enable_read_only_mode(args.disk_id)

    try:
        with MountedVolume(args.disk_id, args.device, args.iam_token_file, args.mount_delay_ms) as nbd:
            if args.device_offset:
                with LoopDev(nbd.path, args.device_offset) as loop_dev:
                    dump_disk_info(loop_dev.path, descr["bs"], output_dir)
                    fix_meta(
                        os.path.join(output_dir, 'meta.json'),
                        args.device_offset)
            else:
                dump_disk_info(nbd.path, descr["bs"], output_dir)
    finally:
        cli.disable_read_only_mode(args.disk_id)

    used_blocks = generate_used_blocks(os.path.join(output_dir, 'meta.json'))

    if used_blocks is None:
        logging.warning(f'empty used blocks map for {args.disk_id}')
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
