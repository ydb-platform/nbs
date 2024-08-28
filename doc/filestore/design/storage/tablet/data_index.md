# Data index in Filestore

Data index is represented by the following layers:
* FreshBytes layer - used for unaligned writes (predominantly smaller than BlockSize)
* FreshBlocks layer - used for small aligned writes
* MixedBlocks layer - used for large aligned writes, aligned parts of large unaligned writes, writes done by background ops (Flush, Compaction, FlushBytes)
* [proposed] LargeFileBlocks layer - planned for use for file blocks at offsets beyond the first N MiBs of each file

## FreshBytes
* Fully cached in memory (represented by a [segment tree-like structure](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/model/fresh_bytes.h)). Loaded upon tablet startup.
* Persistently stored in the FreshBytes localdb table.
* If this layer grows too big in size, it gets flushed via the [FlushBytes operation](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/tablet_actor_flush_bytes.cpp) which works similar to the [Compaction operation](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/tablet_actor_compaction.cpp) but applies FreshBytes to the resulting blobs.

## FreshBlocks
* Fully cached in memory (represented by a [tree-like structure](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/model/fresh_blocks.h)). Loaded upon tablet startup.
* Persistently stored in the FreshBlocks localdb table.
* If this layer grows too big in size, it gets flushed via the [Flush operation](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/tablet_actor_flush.cpp) which simply groups FreshBlocks into larger blobs, writes those blobs to the MixedBlocks layer and clears the processed FreshBlocks.

## MixedBlocks
It is the main layer intended for long-term data storage.
* Contains block-aligned data.
* Not cached in memory (or at least not fully cached). Some of the indexing information may be cached.
* The indexing information is persistently stored in the MixedBlocks and DeletionMarkers localdb tables.
* Split into buckets called "ranges". Splitting is performed via [hashing](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/model/block.cpp).
* Each indexed blob is contained fully within a single range.
* Each blob may contain blocks belonging to multiple different inodes.
* Each range is supposed to contain up to 4MiB of data (sometimes slightly more due to hash collisions).
* Each write generates DeletionMarkers which are later applied to the MixedBlocks table via the [Cleanup operation](https://github.com/ydb-platform/nbs/blob/main/cloud/filestore/libs/storage/tablet/tablet_actor_cleanup.cpp).

## [proposed] LargeFileBlocks
This layer is planned for use with large files. For simplicity the code is going to use it for the blocks at offsets >= N MiB belonging to all files. The first version will implement only large DeletionMarkers to solve the problem of quick large file deletion which is hard to do via the MixedBlocks layer since deleting a 1TiB file implies writing 1TiB / 256KiB = 2 ^ 22 DeletionMarkers in a single transaction.
* The first version is going to load all such large DeletionMarkers into memory upon tablet startup.
* Each large DeletionMarker will describe a range of up to 1GiB
* Large DeletionMarkers will be generated only upon large (> 128GiB) truncate ops.
* Cleanup operation will keep track of the number of large DeletionMarkers and will start applying and trimming them if their total number becomes too big (e.g. greater than, say, 2 ^ 20).
