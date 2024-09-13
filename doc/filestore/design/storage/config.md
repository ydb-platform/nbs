# Storage layer configuration

## What it looks like

StorageConfig is represented by a [proto spec](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/config/storage.proto) like any other filestore and blockstore config. Supplied to the daemon as a prototext file - usually called nfs-storage.txt.

There is a way to to override some of the config settings individually for a single tablet (single FS or FS shard).

Example (overrides CompactionThreshold and CleanupThreshold fields):
```
filestore-client executeaction --action changestorageconfig --input-json '{"FileSystemId": "your_fs_id", "StorageConfig": {"CompactionThreshold": 100, "CleanupThreshold": 10000}}'
```

The overrides can be viewed like this:
```
filestore-client executeaction --action getstorageconfigfields --input-json '{FileSystemId: "your_fs_id", StorageConfigFields: ["CompactionThreshold","CleanupThreshold"]}'
```

## Recommended settings

Most of the fields have reasonable defaults. But in order to keep the behaviour for the existing systems stable from release to release, some of the new features/settings are disabled by default and thus should be manually enabled to get the best performance.

Here is the list of these settings:
* `TwoStageReadEnabled: true` - makes filestore-vhost fetch only metadata from the tablet and read the data directly from storage nodes
* `ThreeStageWriteEnabled: true` - makes filestore-vhost send only the metadata to the tablet upon writes and write the data directly to the storage nodes
* `NewCompactionEnabled: true` - enables a lot more sophisticated Compaction triggers like per-range and per-FS garbage level triggers and per-FS blob count trigger
* `NewCleanupEnabled: true` - similar thing for Cleanup - enables per-FS deletion marker count trigger
* `ReadAheadCacheRangeSize: 1048576` - enables index readahead for ranges up to 1MiB if a read pattern which is similar to sequential read is spotted (significantly improves performance for small and almost sequential reads)
* `NodeIndexCacheMaxNodes: 128` - enables node metadata index in the tablet (e.g. index for GetNodeAttr (stat) responses)
* `PreferredBlockSizeMultiplier: 64` - scales the recommended BlockSize shown to the guest which makes some apps like `cat` use larger read request sizes which optimizes their read throughput
* `MultiTabletForwardingEnabled: true` - basically enables the multitablet FS (#1350) feature
* `GetNodeAttrBatchEnabled: true` - enables fetching NodeAttr (stat) in large batches for multitablet filesystems
* `UnalignedThreeStageWriteEnabled: true` - causes unaligned writes to follow the efficient ThreeStageWrite datapath
* `UseMixedBlocksInsteadOfAliveBlocksInCompaction: true` - see the description in storage.proto (this flag basically fixes garbage level-based compaction triggers)
