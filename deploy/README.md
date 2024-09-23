# Sample configs for cluster deployment

## Configs
There is a sample 8-node cluster configuration in the 8_node_cluster folder. It supposes that you have 8 hosts with fqdns:
* my.host1
* my.host2
* my.host3
* my.host4
* my.host5
* my.host6
* my.host7
* my.host8

Those hosts should have a bunch of disks which should be available at the following paths:
* /dev/disk/by-partlabel/NVMEKIKIMR01 - required
* /dev/disk/by-partlabel/NVMEKIKIMR0[2-3] - optional
* /dev/disk/by-partlabel/ROTKIKIMR0[2-3] - optional
* /dev/disk/by-partlabel/ROTNBS[01-99] - optional
* /dev/disk/by-partlabel/NVMENBS[01-99] - optional

The cluster is called "my_cluster".

The configs include both NBS and Filestore.

## Build and run
See the [instructions](/example/README.md) to find out how to build and run NBS binaries.
See the [instructions](/cloud/filestore/README.md) to find out how to build and run Filestore binaries.
