import json
import subprocess as sp
import optparse


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('--disk-id')
    parser.add_option('--new-type')
    parser.add_option('--apply', default=False, action='store_true')
    return parser.parse_args()


def exec(cmd):
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        s = 'Failed to run {} {} return code: {}'.format(
            stdout, stderr, p.returncode)
        raise Exception(s)

    return stdout.decode('utf-8')


def translate_media_kind(media_kind):
    if media_kind == 1:
        return 'ssd'
    elif media_kind == 2:
        return 'hybrid'
    elif media_kind == 3:
        return 'hdd'
    elif media_kind == 4:
        return 'nonreplicated'
    elif media_kind == 5:
        return 'mirror2'
    elif media_kind == 7:
        return 'mirror3'
    elif media_kind == 8:
        return 'hdd_nonrepl'
    else:
        raise Exception('Unknown media kind {}'.format(media_kind))


def describe_disk(opts):
    input = {"DiskId": opts.disk_id, "ExactDiskIdMatch": False}
    cmd = [
        './blockstore-client.sh', 'executeaction',
        '--action=describevolume',
        '--input-bytes={}'.format(json.dumps(input))
    ]
    print(cmd)
    allLines = exec(cmd)
    for line in allLines.split('\n'):
        try:
            return json.loads(line)
        except Exception as e:
            continue
    raise Exception('Failed to find disk ' + opts.disk_id)


def get_disk_info(described_disk):
    volume_config = described_disk['VolumeConfig']
    diskId = volume_config['DiskId']
    cloudId = volume_config['CloudId']
    folderId = volume_config['FolderId']
    blockSize = int(volume_config['BlockSize'])
    storageMediaKind = translate_media_kind(volume_config['StorageMediaKind'])
    placementGroupId = volume_config['PlacementGroupId']
    placementPartitionIndex = volume_config['PlacementPartitionIndex']

    blockCount = 0
    for partition in volume_config['Partitions']:
        blockCount += int(partition['BlockCount'])

    r = dict()
    r["diskId"] = diskId
    r["cloudId"] = cloudId
    r["folderId"] = folderId
    r["blockSize"] = blockSize
    r["blockCount"] = blockCount
    r["storageMediaKind"] = storageMediaKind
    r["placementGroupId"] = placementGroupId
    r["placementPartitionIndex"] = placementPartitionIndex
    return r


def make_copy_info(volume, opts):
    copy = volume.copy()

    diskId = volume["diskId"]
    if diskId.endswith("-copy"):
        diskId = volume["diskId"][:-5]
    else:
        diskId += "-copy"
    copy["diskId"] = diskId

    if opts.new_type is not None:
        copy["storageMediaKind"] = opts.new_type

    if copy["storageMediaKind"] != "nonreplicated":
        copy["placementGroupId"] = ""
        copy["placementPartitionIndex"] = ""

    return copy


def copy_disk(src, dst):
    tags = "source-disk-id={}".format(src["diskId"])
    create_cmd = [
        './blockstore-client.sh',
        'CreateVolume',
        '--disk-id={}'.format(dst["diskId"]),
        '--storage-media-kind={}'.format(dst["storageMediaKind"]),
        '--blocks-count={}'.format(dst["blockCount"]),
        '--block-size={}'.format(dst["blockSize"]),
        '--folder-id={}'.format(dst["folderId"]),
        '--cloud-id={}'.format(dst["cloudId"]),
        '--placement-group-id={}'.format(dst["placementGroupId"]),
        '--placement-partition-index={}'.format(
            dst["placementPartitionIndex"]),
        '--tags={}'.format(tags)
    ]
    print(create_cmd)
    exec(create_cmd)

    link_cmd = [
        './blockstore-client.sh',
        'CreateVolumeLink',
        '--leader-disk-id={}'.format(src["diskId"]),
        '--follower-disk-id={}'.format(dst["diskId"])
    ]
    print(link_cmd)
    exec(link_cmd)


def main(opts):
    described_disk = describe_disk(opts)
    # print(json.dumps(described_disk, indent=4))
    src = get_disk_info(described_disk)
    dst = make_copy_info(src, opts)
    print(src)
    print(dst)
    if opts.apply:
        copy_disk(src, dst)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts)
