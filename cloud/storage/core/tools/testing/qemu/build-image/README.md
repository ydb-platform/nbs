### Introduction

This script allows to build qcow2 image for local and cloud tests based on vanilla ubuntu. Main purpose is to customize image with required packages and cloud init. The main difference between local and cloud image is allowing to use plain text authentication which is also very useful for running qemu localy.


### Preparing tools

```
sudo apt install -y genisoimage qemu-system qemu-utils qemu-efi-aarch64
```

### Building for cloud tests

- Create image

```
./build-image --user root --ssh-key nbs-key --out ubuntu-2004-eternal.qcow
```

- Upload it to the s3 bucket

```
IAM_TOKEN="ycp iam create token"
FILE="ubuntu-2004-eternal.qcow"
BUCKET="nbs-tests-common"

curl -o /dev/null -# https://$BUCKET.storage.cloudil.com/$FILE -T $FILE -H "X-YaCloud-SubjectToken: $IAM_TOKEN" -X PUT
```

- Create a new image out of s3 uploaded file via console

- Update it for required cluster in tests configs

### Building for local tests

- Create image

```
./build-image --with-rdma --plain-pwd --ssh-key ../keys/id_rsa --out rootfs.amd64.img
```

- Optionally compress image
```
qemu-img convert -c -p -f qcow2 -O qcow2 rootfs.img rootfs-compressed.img
```

- Upload it to the sandbox

```
ya upload --ttl=inf -T NBS_QEMU_DISK_IMAGE -d 'Customized Ubuntu Cloud Image in QCOW2 format'
```

- Update [resource](https://github.com/ydb-platform/nbs/blob/main/cloud/storage/core/tools/testing/qemu/image/ya.make) in ya.make

### Building for arm tests

Use ports repo http://ports.ubuntu.com or mirror
```
./build-image --arch arm64 --repo-mirror https://mirror.yandex.ru/ubuntu-ports
```
