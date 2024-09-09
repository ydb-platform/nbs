# Background ops in Filestore Tablet

Since the data is stored in immutable blobs, we need some background operations whose main jobs would be:
* data compaction - making the data that is logically local (e.g. adjacent blocks in a single file) be physically local (stored in the same blob) - makes read requests faster, makes index smaller
* garbage collection - finding and rewriting all blobs the data in which is logically overwritten by a write request upon every write request is expensive, so each write is actually "blind" - it only writes new data and the data that gets overwritten is usually detected and processed later - in background

The main background ops are:
* Compaction
* Cleanup
* Flush
* FlushBytes
* CollectGarbage

These operations are triggered based on various statistics - MaxBlobsPerRange, MaxDeletionsPerRange, etc.
Ops are triggered when the related statistics are higher than some threshold. The thresholds are specified in TStorageServiceConfig (usually contained in a file named nfs-storage.txt).
If the values of some statistics are way too high, new WriteData / GenerateBlobIds requests get rejected (receive E_REJECTED error) - so-called "Backpressure" logic.
Backpressure is needed in order not to degrade into a state when the amount of garbage and/or size of the index is so big that it becomes unmanageable - e.g. localdb transactions start to hit size limits, localdb overall starts to be too slow, the overall amount of garbage becomes unreasonably high, etc.

## Compaction

Goals:
* make [MixedIndex](data_index.md#mixedblocks) as small as possible
* rewrite data to improve data locality
* get rid of overwritten data

What is does:
* selects MixedIndex range with the most blobs in it, scans that range to extract blocklists and blobs
* rewrites the blocks from that range throwing away the overwritten blocks, generates new blobs in the same range

Is triggered:
* either when blob count in some range exceeds some threshold
* or when average garbage percentage per range exceeds some threshold

## Cleanup

Goals:
* minimize DeletionMarker count
* get rid of blobs containing only overwritten data

What it does:
* selects MixedIndex range with the most DeletionMarkers in it
* OR selects MixedIndex range from the so-called PriorityRangesForCleanup queue which is filled if the number of LargeDeletionMarkers (which are global, not local to one of the ranges) is too big
* applies DeletionMarkers from this range to blob BlockLists in this range
* deletes the processed DeletionMarkers (both Mixed and Large) from the index

Is triggered:
* either when DeletionMarker count in some range exceeds some threshold
* or when average deletion marker count per range exceeds some threshold
* or if LargeDeletionMarker count exceeds some threshold

## Flush

Goals:
* rewrite [FreshBlocks](data_index.md#freshblocks) as larger MixedBlobs
* trim FreshBlocks

What it does:
* reads FreshBlocks, groups them into larger blobs
* writes those blobs (via WriteBlob requests) and adds them to the index (via AddBlob requests)

Is triggered:
* when the total size of FreshBlocks exceeds some threshold

## FlushBytes

Goals:
* apply [FreshBytes](data_index.md#freshbytes) to the blobs with which those byte ranges intersect
* trim FreshBytes

What it does:
* takes some ranges from FreshBytes (usually all ranges)
* scans MixedIndex to find the blobs with which those ranges intersect
* reads those blobs, writes the corresponding FreshBytes on top of them, writes the results as new blobs (similar to Compaction)

Is triggered:
* when the total size of FreshBytes exceeds some threshold

## CollectGarbage

Goals:
* move GC barriers in BlobStorage groups
* set Keep / DontKeep flags for blobs

What it does:
* reads GarbageBlobs and NewBlobs from the queue, deduplicates them
* calculates barrier values per group, sends EvCollectGarbage requests
* calls DeleteGarbage op which cleans up GarbageQueue

Is triggered:
* when GarbageQueueSize (size of the blobs in GarbageQueue, i.e. GarbageBlobs + NewBlobs) exceeds some threshold
