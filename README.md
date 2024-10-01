## Network Block Store and Network File Store

Network Block Device implementation over YDB BlobStorage or over our own storage nodes. Offers reliable thin-provisioned block devices which support snapshots.
Network File System implementation over YDB BlobStorage. Offers POSIX-compliant scalable filesystem which can be attached to virtual machines via virtiofs or simply mounted via FUSE.

### Quickstart

Follow the instructions [here](example/README.md) to build and run NBS on your machine and to attach an NBS-based disk via NBD. NBS-based disks can be attached via vhost-user-blk as well.

Follow the instructions [here](cloud/filestore/README.md) to build and run Filestore on your machine and to attach it to a virtual machine via virtiofs or mount it on your host via FUSE.

Follow the instructions [here](CLANG-FORMAT.md) to install clang-format for formatting the code.

### How to Deploy

Follow the instructions [here](deploy/README.md) to deploy NBS and Filestore on a cluster.
