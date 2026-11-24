import base64
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
    6: 'local',
    7: 'mirror3',
    8: 'hdd_nonrepl',
    9: 'hdd_local',
}

MEDIA_KIND_IDS = {
    **{name: kind for kind, name in MEDIA_KINDS.items()},
    'ssd_nonrepl': 4,
    'ssd_mirror2': 5,
    'ssd_local': 6,
    'ssd_mirror3': 7,
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
    r["tabletVersion"] = volume_config.get("TabletVersion") or 1
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


def create_disk(dst, tags, apply, source_encryption):
    mode = source_encryption.get("Mode", NO_ENCRYPTION)
    if mode not in (NO_ENCRYPTION, ENCRYPTION_AES_XTS):
        raise ValueError(f'Copying disks with encryption mode {mode} is unsupported')

    media_kind = dst["storageMediaKind"]
    if mode == ENCRYPTION_AES_XTS:
        if media_kind not in MEDIA_KIND_IDS:
            raise ValueError(f'Unknown media kind {media_kind}')
        media_kind = MEDIA_KIND_IDS[media_kind]

    fields = [
        ("DiskId", "disk-id", dst["diskId"]),
        ("StorageMediaKind", "storage-media-kind", media_kind),
        ("BlocksCount", "blocks-count", dst["blockCount"]),
        ("BlockSize", "block-size", dst["blockSize"]),
        ("FolderId", "folder-id", dst["folderId"]),
        ("CloudId", "cloud-id", dst["cloudId"]),
        ("TabletVersion", "tablet-version", dst["tabletVersion"]),
        ("TagsStr", "tags", tags),
    ]
    if dst["placementGroupId"] != '':
        fields.append(("PlacementGroupId", "placement-group-id", dst["placementGroupId"]))
    if dst["placementPartitionIndex"] != '':
        fields.append((
            "PlacementPartitionIndex",
            "placement-partition-index",
            int(dst["placementPartitionIndex"]),
        ))

    cmd = [BLOCKSTORE_CLIENT, 'CreateVolume']
    if mode == NO_ENCRYPTION:
        cmd.extend(f'--{flag}={value}' for _, flag, value in fields)
        print(cmd)
        if apply:
            exec(cmd)
        return

    cmd.extend(['--proto', '--verbose', 'error'])
    request = ''.join(
        f'{field}: {json.dumps(value, ensure_ascii=False)}\n'
        for field, _, value in fields
    )
    source_key_hash = source_encryption.get("KeyHash") or ""
    request += 'EncryptionSpec { Mode: ENCRYPTION_AES_XTS'
    if source_key_hash:
        # DescribeVolume returns bytes as base64. Protobuf text takes raw bytes.
        digest = base64.b64decode(source_key_hash, validate=True)
        encoded = ''.join(f'\\{byte:03o}' for byte in digest)
        request += f' KeyHash: "{encoded}"'
    request += ' }\n'

    print(cmd)
    if not apply:
        print(request, end='')
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
    if (actual.get("KeyHash") or "") != source_key_hash:
        raise RuntimeError(f'Destination disk {dst["diskId"]} has a different key hash')


def copy_disk(src, dst, opts, source_encryption):
    tags = "source-disk-id={}".format(src["diskId"])
    create_disk(dst, tags, opts.apply, source_encryption)

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
