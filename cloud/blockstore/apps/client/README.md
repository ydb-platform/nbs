# Network block store command line client

## Common options

 * ```--config``` - path to protobuf file with NBS client configuration, not mandatory
 * ```--host``` - host address where NBS server contacted by client works; not mandatory, taken from config by default, localhost if config is not specified
 * ```--port``` - port on which NBS server contacted by client is listening; not mandatory, taken from config by default
 * ```--mon-file``` - path to protobuf file with monitoring configuration, not mandatory
 * ```--mon-address``` - host address where monitoring service works; not mandatory, taken from monitoring config by default
 * ```--mon-port``` - port on which monitoring service is listening; not mandatory, taken from monitoring config by default
 * ```--mon-threads``` - number of threads for monitoring; not mandatory, taken from monitoring config by default
 * ```--verbose``` - level of logging; possible values: ```error```, ```warn```, ```info```, ```debug```, ```resource```; the default is ```info```
 * ```--proto``` - option switching the client to the mode in which it accepts the request to execute from input as protobuf text and outputs the response as protobuf text
 * ```--input``` - path to a file from which request data should be read: buffers for ```WriteBlocks``` request or whole request protobuf if ```--proto``` option is specified
 * ```--output``` - path to a file where response data should be written: buffers for ```ReadBlocks``` request or whole response protobuf if ```--proto``` option is specified
 * free argument: the name of request to execute; can be written either in camel case (i.e. ```WriteBlocks```) or as a single lowercase or uppercase word (i.e. ```writeblocks```) or with words separated by hyphens or underscores (i.e. ```write-blocks```).
 * ```--server-unix-socket-path``` - path to a socket on which the NBS server contacted by the client is listening; not mandatory

## Options for particular commands

### Ping

No options are required.

### CreateVolume

 * ```--disk-id``` - name of volume to create; **mandatory**
 * ```--block-size``` - minimum addressable block size (smallest unit of I/O operations); not mandatory, defaults to 4096 bytes
 * ```--blocks-count``` - number of blocks per volume; not mandatory, defaults to 0
 * ```--project-id``` - project identifier; not mandatory
 * ```--storage-media-kind``` - either of the following: ```ssd```, ```hdd```, ```hybrid```, ```ssd_nonrepl```, ```hdd_nonrepl```, ```ssd_mirror2```, ```ssd_mirror3```, ```ssd_local```, ```hdd_local```; not mandatory, defaults to ```hdd```
 * ```--folder-id``` - folder identifier; not mandatory
 * ```--cloud-id``` - cloud identifier; not mandatory
 * ```--performance-profile-max-iops``` - max iops; not mandatory
 * ```--tablet-version``` - either 1 or 2; not mandatory, defaults to 1
 * ```--base-disk-id``` - name of base volume; not mandatory
 * ```--base-disk-checkpoint-id``` - checkpoint identifier of base volume; not mandatory

### DestroyVolume

 * ```--disk-id``` - name of volume to destroy; **mandatory**

### AlterVolume

 * ```--disk-id``` - name of volume to alter; **mandatory**
 * ```--project-id``` - project identifier; not mandatory
 * ```--folder-id``` - folder identifier; not mandatory
 * ```--cloud-id``` - cloud identifier; not mandatory
 * ```--performance-profile-max-iops``` - max iops; not mandatory
 * ```--config-version``` - volume config version; not mandatory

### ResizeVolume

 * ```--disk-id``` - name of volume to resize; **mandatory**
 * ```--blocks-count``` - number of blocks per volume; **mandatory**
 * ```--config-version``` - volume config version; not mandatory

### AssignVolume

 * ```--disk-id``` - name of volume to assign token for; **mandatory**
 * ```--token``` - mount token for volume; **mandatory**
 * ```--token-host``` - host for which token is assigned to volume; not mandatory
 * ```--instance-id``` - VM information; not mandatory

### DescribeVolume

 * ```--disk-id``` - name of volume ot describe; **mandatory**

### StatVolume

 * ```--disk-id``` - name of volume to get stat for; **mandatory**
 * ```--flags``` - flags describing query; not mandatory

### ListVolumes

No options are required.

### CreateCheckpoint

 * ```--disk-id``` - name of volume for which the checkpoint is to be created; **mandatory**
 * ```--checkpoint-id``` - name of checkpoint to create; **mandatory**

### DeleteCheckpoint

 * ```--disk-id``` - name of volume which checkpoint is to be deleted; **mandatory**
 * ```--checkpoint-id``` - name of checkpoint ot delete; **mandatory**

### GetChangedBlocks

 * ```--disk-id``` - name of volume which changed blocks are queried; **mandatory**
 * ```--start-index``` - index of the first block to consider; **mandatory**
 * ```--blocks-count``` - number of blocks to consider; **mandatory**
 * ```--low-checkpoint-id``` - name of first checkpoint to consider; not mandatory
 * ```--high-checkpoint-id``` - name of last checkpoint to consider; not mandatory

### ReadBlocks

 * ```--disk-id``` - name of volume from which blocks are to be read; **mandatory**
 * ```--start-index``` - index of the first block to read; **mandatory** unless ```--read-all``` option is used in which case start index is always considered 0 regardless of what value is specified, if any
 * ```--blocks-count``` - number of blocks to read; **mandatory** unless ```--read-all``` option is used in which case blocks count is considered equal to the number of block within the volume regardless of what value is specified, if any
 * ```--token``` - mount token assigned to the volume; not mandatory
 * ```--checkpoint-id``` - name of checkpoint to read blocks from; not mandatory
 * ```--read-all``` - read the whole content of volume i.e. all its blocks, from first to last inclusive; **use with caution**; this option can only be used if ```--proto``` option is not used

ReadBlocks requests larger than 1000 blocks are automatically split into several ones, each no larger than 1000 blocks unless ```--proto``` option is used in which case the input request is executed as is.

## WriteBlocks

 * ```--disk-id``` - name of volume to write blocks onto; **mandatory**
 * ```--start-index``` - index of the first block to write; not mandatory, defaults to 0 if not set
 * ```--token``` - mount token assigned to the volume; not mandatory

WriteBlocks requests larger than 1000 blocks are automatically split into several ones, each no larger than 1000 blocks unless ```--proto``` option is used in which case the input request is executed as is.

### ZeroBlocks

 * ```--disk-id``` - name of volume to zero blocks of; **mandatory**
 * ```--start-index``` - index of the first block to zero; **mandatory** unless ```--zero-all`` option is used in which case start index is always considered 0 regardless of what value is specified, if any
 * ```--blocks-count``` - number of blocks to zero; **mandatory** unless ```--zero-all`` option is used in which case blocks count is considered equal to the number of blocks within the volume regardless of what value is specified, if any
 * ```--token``` - mount token assigned to the volume; not mandatory
 * ```--zero-all``` - zero all blocks within the volume, from first to last inclusive; **use with caution**; this option can only be used if ```--proto``` option is not used

## Private API

### ExecuteAction

 * ```--action``` - name of action to execute; **mandatory**
 * ```--input-bytes``` - input bytes; not mandatory
