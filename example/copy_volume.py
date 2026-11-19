import time
import json
import subprocess as sp
import optparse
import re

BLOCKSTORE_CLIENT = './blockstore-client.sh'
NO_ENCRYPTION = 0
ENCRYPTION_AES_XTS = 1

MEDIA_KINDS = {
    1: 'ssd',
    2: 'hybrid',
    3: 'hdd',
    4: 'nonreplicated',
    5: 'mirror2',
    7: 'mirror3',
    8: 'hdd_nonrepl',
}

MEDIA_KIND_IDS = {
    **{name: kind for kind, name in MEDIA_KINDS.items()},
    'ssd_nonrepl': 4,
    'ssd_mirror2': 5,
    'local': 6,
    'ssd_local': 6,
    'ssd_mirror3': 7,
    'hdd_local': 9,
    'ssd_direct_mirror3of5_group': 10,
    'ssd-direct-mirror3of5-group': 10,
}


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('--disk-id')
    parser.add_option('--new-type')
    parser.add_option('--apply', default=False, action='store_true')
    return parser.parse_args()


def exec(cmd, request=None):
    result = sp.run(
        cmd, input=request, stdout=sp.PIPE, stderr=sp.STDOUT, encoding='utf-8')
    if result.returncode != 0:
        raise RuntimeError(
            f'Command failed with exit code {result.returncode}: {result.stdout}')
    return result.stdout


def execAction(action, input):
    cmd = [
        BLOCKSTORE_CLIENT,
        'ExecuteAction',
        '--action={}'.format(action),
        '--input-bytes={}'.format(json.dumps(input))
    ]
    print(cmd)
    allLines = exec(cmd)
    for line in allLines.split('\n'):
        try:
            return json.loads(line)
        except Exception as e:
            continue
    raise Exception('Failed to execute action {}' + action)


def translate_media_kind(media_kind):
    if media_kind not in MEDIA_KINDS:
        raise ValueError(f'Unknown media kind {media_kind}')
    return MEDIA_KINDS[media_kind]


def describe_disk(opts):
    input = {"DiskId": opts.disk_id, "ExactDiskIdMatch": False}
    return execAction('DescribeVolume', input)


def get_disk_info(described_disk):
    volume_config = described_disk['VolumeConfig']
    diskId = volume_config['DiskId']
    cloudId = volume_config['CloudId']
    folderId = volume_config['FolderId']
    blockSize = int(volume_config['BlockSize'])
    storageMediaKind = translate_media_kind(volume_config['StorageMediaKind'])
    placementGroupId = volume_config['PlacementGroupId']
    if 'PlacementPartitionIndex' in volume_config:
        placementPartitionIndex = volume_config['PlacementPartitionIndex']
    else:
        placementPartitionIndex = ''

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


def create_aes_xts_disk(dst, tags, apply, source_key_hash):
    media_kind = dst["storageMediaKind"]
    if media_kind not in MEDIA_KIND_IDS:
        raise ValueError(f'Unknown media kind {media_kind}')

    fields = {
        "DiskId": dst["diskId"],
        "StorageMediaKind": MEDIA_KIND_IDS[media_kind],
        "BlocksCount": dst["blockCount"],
        "BlockSize": dst["blockSize"],
        "FolderId": dst["folderId"],
        "CloudId": dst["cloudId"],
        "TabletVersion": 1,
        "TagsStr": tags,
    }
    if dst["placementGroupId"] != '':
        fields["PlacementGroupId"] = dst["placementGroupId"]
    if dst["placementPartitionIndex"] != '':
        fields["PlacementPartitionIndex"] = int(dst["placementPartitionIndex"])

    # The first encrypted mount supplies the key hash. The CLI encryption
    # options require a key or hash, so send the mode through the proto input.
    request = ''.join(
        f'{field}: {json.dumps(value, ensure_ascii=False)}\n'
        for field, value in fields.items()
    ) + 'EncryptionSpec { Mode: ENCRYPTION_AES_XTS }\n'

    cmd = [BLOCKSTORE_CLIENT, 'CreateVolume', '--proto', '--verbose', 'error']
    print(cmd)
    print(request, end='')
    if not apply:
        return

    response = exec(cmd, request)
    # Proto mode reports RPC errors in the response despite exiting zero.
    error = re.search(r'^Error\s*\{\s*Code:\s*(\d+)', response, re.MULTILINE)
    if error and int(error.group(1)) & (1 << 31):
        raise RuntimeError(f'CreateVolume failed for {dst["diskId"]}: {response}')

    # CreateVolume may reuse an existing disk without comparing encryption.
    actual = execAction('DescribeVolume', {
        "DiskId": dst["diskId"], "ExactDiskIdMatch": True,
    })["VolumeConfig"].get("EncryptionDesc", {})
    if actual.get("Mode") != ENCRYPTION_AES_XTS:
        raise RuntimeError(f'Destination disk {dst["diskId"]} must use AES-XTS')
    if actual.get("KeyHash") and actual["KeyHash"] != source_key_hash:
        raise RuntimeError(f'Destination disk {dst["diskId"]} has a different key hash')


def create_unencrypted_disk(dst, tags, apply):
    create_cmd = [
        BLOCKSTORE_CLIENT,
        'CreateVolume',
        '--disk-id={}'.format(dst["diskId"]),
        '--storage-media-kind={}'.format(dst["storageMediaKind"]),
        '--blocks-count={}'.format(dst["blockCount"]),
        '--block-size={}'.format(dst["blockSize"]),
        '--folder-id={}'.format(dst["folderId"]),
        '--cloud-id={}'.format(dst["cloudId"]),
        '--tags={}'.format(tags)
    ]
    if dst["placementGroupId"] != '':
        create_cmd.append('--placement-group-id={}'.format(dst["placementGroupId"]))
    if dst["placementPartitionIndex"] != '':
        create_cmd.append('--placement-partition-index={}'.format(dst["placementPartitionIndex"]))

    print(create_cmd)
    if apply:
        exec(create_cmd)


def copy_disk(src, dst, opts, source_encryption):
    tags = "source-disk-id={}".format(src["diskId"])
    mode = source_encryption.get("Mode", NO_ENCRYPTION)
    if mode == ENCRYPTION_AES_XTS:
        create_aes_xts_disk(dst, tags, opts.apply, source_encryption.get("KeyHash"))
    elif mode == NO_ENCRYPTION:
        create_unencrypted_disk(dst, tags, opts.apply)
    else:
        raise ValueError(f'Copying disks with encryption mode {mode} is unsupported')

    link_cmd = [
        BLOCKSTORE_CLIENT,
        'CreateVolumeLink',
        '--leader-disk-id={}'.format(src["diskId"]),
        '--follower-disk-id={}'.format(dst["diskId"])
    ]
    print(link_cmd)
    if opts.apply:
        exec(link_cmd)


def get_link_status(src, dst):
    input = {"LeaderDiskId": src["diskId"], "FollowerDiskId": dst["diskId"]}
    while True:
        status = execAction('GetLinkStatus', input)
        print(status)

        if status["Status"] == "LINK_STATUS_COMPLETED" \
                or status["Status"] == "LINK_STATUS_ERROR" \
                or status["Status"] == "LINK_STATUS_NOT_FOUND":
            break
        time.sleep(2)


def main(opts):
    described_disk = describe_disk(opts)
    # print(json.dumps(described_disk, indent=4))
    src = get_disk_info(described_disk)
    dst = make_copy_info(src, opts)
    print(src)
    print(dst)
    source_encryption = described_disk["VolumeConfig"].get("EncryptionDesc", {})
    copy_disk(src, dst, opts, source_encryption)
    get_link_status(src, dst)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts)
