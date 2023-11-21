### Introduction

This script allows to build qcow2 image for local and cloud tests based on vanilla ubuntu. Main purpose is to customize image with required packages and cloud init. The main difference between local and cloud image is allowing to use plain text authentication which is also very useful for running qemu localy.

### Building for cloud tests

- Create image

```
./build-image --user root --ssh-key [nbs key](https://yav.yandex-team.ru/secret/sec-01g6n81wt8wc6ph8kagb5b33bf/explore/version/ver-01g6n81wtp52w8pacjp41xhrpg) --out ubuntu-2004-eternal.qcow
```

- Upload it to the s3 bucket

```
IAM_TOKEN="ycp iam create token"
FILE="ubuntu-2004-eternal.qcow"
BUCKET="nbs-tests-common"

curl -o /dev/null -# https://$BUCKET.storage.cloudil.com/$FILE -T $FILE -H "X-YaCloud-SubjectToken: $IAM_TOKEN" -X PUT
```

- Create a new image out of s3 uploaded file via console

- Update it for required cluster in [tests configs](https://a.yandex-team.ru/arcadia/cloud/blockstore/pylibs/clusters/test_config.py?rev=r10300383#L41-232)

### Building for local tests

- Create image

```
./build-image --user qemu --plain-pwd --ssh-key [nbs key](https://yav.yandex-team.ru/secret/sec-01g6n81wt8wc6ph8kagb5b33bf/explore/version/ver-01g6n81wtp52w8pacjp41xhrpg) --out rootfs.img
```

- Optionally compress image
```
qemu-img convert -c -p -f qcow2 -O qcow2 rootfs.img rootfs-compressed.img
```

- Upload it to the sandbox

```
ya upload --ttl=inf -T NBS_QEMU_DISK_IMAGE -d 'Customized Ubuntu Cloud Image in QCOW2 format'
```

- Update [resource](https://a.yandex-team.ru/arcadia/cloud/storage/core/tools/testing/qemu/image/ya.make?rev=r9706918#L8) in ya.make
