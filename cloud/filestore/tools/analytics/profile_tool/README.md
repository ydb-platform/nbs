# Filestore command line tool for dumping and analyzing profile log

Profile log fills in three places:

* [Public API requests](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/service/request.h) are partially filled [on execute](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/server/server.cpp) in server and [on sending response](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/server/server.cpp). It will be dumped to profile-log [on complete](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/server/server.cpp). At the present time this approach is not used.

* On client-side (vhost) [public API](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/service/request.h) requests are partially filled [on request forwarding](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/service/service_actor_forward.cpp) in service actor and [on request completion](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/service/service_actor_complete.cpp). It will be dumped to profile-log [on in-flight request completion](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/service/service_state.cpp). Only significant requests with filled additional fields will be dumped. This profile log can be found in ```/var/log/nfs/vhost-server.log``` on hosts.

* In tablet [private API](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/model/profile_log_events.h) requests are filled in several places. You can try to grep ```InitProfileLogRequestInfo``` and ```FinalizeProfileLogRequestInfo``` in ```cloud/filestore/libs/storage/tablet/*``` and find all the places. This profile log will be dumped to ```/var/log/nfs/nfs-profile.log``` on nfs-control svms.


## Common options

| Command line parameter | Data type | Description |
|:----------------------:|:---------:|-------------|
| ```--profile-log```    | String    | Path to log file with NFS binary profile log |
| ```--fs-id```          | String    | Filesystem Id, used to output events only for specified filesystem. |
| ```--node-id```        | ui64      | Node id (a.k.a. inode), output events which were applied to specified node id |
| ```--handle```         | ui64      | Handle id, output events which used specified handle id |
| ```--since```          | Timestamp | [ISO 8601:2004 format](https://www.iso.org/standard/40874.html), output all events from this timestamp |
| ```--until```          | Timestamp | [ISO 8601:2004 format](https://www.iso.org/standard/40874.html), output all events strictly before given timestamp |

## Options for particular commands

### DumpEvents

| Command line parameter | Data type | Description |
|:----------------------:|:---------:|-------------|
| ```--request-type```   | ui32      | Request type id, used to output requests only specified requests |

#### Possible event types

| Event id | Event name |
|:--------:|------------|
| 0        | Ping |
| 1        | CreateFileStore |
| 2        | DestroyFileStore |
| 3        | AlterFileStore |
| 4        | ResizeFileStore |
| 5        | DescribeFileStoreModel |
| 6        | GetFileStoreInfo |
| 7        | ListFileStores |
| 8        | CreateSession |
| 9        | DestroySession |
| 10       | PingSession |
| 11       | AddClusterNode |
| 12       | RemoveClusterNode |
| 13       | ListClusterNodes |
| 14       | AddClusterClients |
| 15       | RemoveClusterClients |
| 16       | ListClusterClients |
| 17       | UpdateCluster |
| 18       | StatFileStore |
| 19       | SubscribeSession |
| 20       | GetSessionEvents |
| 21       | ResetSession |
| 22       | CreateCheckpoint |
| 23       | DestroyCheckpoint |
| 24       | ResolvePath |
| 25       | CreateNode |
| 26       | UnlinkNode |
| 27       | RenameNode |
| 28       | AccessNode |
| 29       | ListNodes |
| 30       | ReadLink |
| 31       | SetNodeAttr |
| 32       | GetNodeAttr |
| 33       | SetNodeXAttr |
| 34       | GetNodeXAttr |
| 35       | ListNodeXAttr |
| 36       | RemoveNodeXAttr |
| 37       | CreateHandle |
| 38       | DestroyHandle |
| 39       | AcquireLock |
| 40       | ReleaseLock |
| 41       | TestLock |
| 42       | ReadData |
| 43       | WriteData |
| 44       | AllocateData |
| 45       | GetSessionEventsStream |
| 46       | StartEndpoint |
| 47       | StopEndpoint |
| 48       | ListEndpoints |
| 49       | KickEndpoint |
| 10001    | Flush |
| 10002    | FlushBytes |
| 10003    | Compaction |
| 10004    | Cleanup |
| 10005    | TrimBytes |
| 10006    | CollectGarbage |
| 10007    | DeleteGarbage |
| 10008    | ReadBlob |
| 10009    | WriteBlob |
| 10010    | AddBlob |
| 10011    | TruncateRange |

### FindBytesAccess

| Command line parameter | Data type | Description |
|:----------------------:|:---------:|-------------|
| ```--start```          | ui64      | Index of the first block of the interval |
| ```--count```          | ui64      | Quantity of blocks in interval |
| ```--block-size```     | ui32      | Block size (in bytes), default is 4096 bytes |

## Examples

* Print all **ReadLink** events for **filesystem=cga2vco9d6ofs7lb2md2** which touched **node-id=123** and **handle=456** since **2023-01-01T10:00:00** until **2023-01-01T11:00:00** from **~/vhost-profile.log**

```./filestore-profile-tool dumpevents --profile-log ~/vhost-profile.log --fs-id cga2vco9d6ofs7lb2md2 --node-id 123 --handle 456 --since "2023-01-01T10:00:00" --until "2023-01-01T11:00:00" --request-type 30```

* Print all events from **~/nfs-profile.log** from range **[128, 192)** with **block-size=8192**

```./filestore-profile-tool findbytesaccess --profile-log ~/nfs-profile.log --start 128 --count 64 --block-size 8192```
