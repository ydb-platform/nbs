# Block Mask Optimization

## Problem

Reading block masks takes a lot of CPU time during the compaction transaction.

<img width="4804" height="916" alt="Image" src="https://github.com/user-attachments/assets/10635421-745f-4e57-8896-8410e2e57d24" />

Block masks are used to track blobs that have been overwritten with other blobs. When the block mask is full, we add a blob to the cleanup queue.

But if a blob lies entirely in one compaction range, it should be overwritten with a new blob during compaction. So we can avoid reading block masks for this type of blob.

## Solution

Skip reading block masks for blobs that lie entirely within a single compaction range, and use merge or mixed index data to recognize those blobs.

## Detailed Design


For mixed blobs everything is more complicateFor merged blobs we already have everything needed for this optimization. In the merge index we have a mapping `[RangeStart, RangeEnd] -> [BlobId, ...]`. So to determine whether a merged blob lies entirely in one compaction range, we can just look at the key.
d. In the mixed index we have a mapping `[BlockIndex] -> [BlobId, Offset, ...]`. So even if we read all mixed blocks in a range, we cannot tell whether there are other blocks in other ranges. We can add a new column to the mixed index, something like `BlobAlignment`: if a blob lies entirely in one compaction range, `BlobAlignment` should be equal to the compaction range size; otherwise it can be zero. Then, while reading blocks during compaction, we can tell whether a blob lies completely in one compaction range or not.

Even with this design we can skip a lot of block mask reads. I ran pgbench on a PG cluster with network SSD disks, measured the compaction range count for every flushed blob, and got the following results:

![Compaction Ranges per flushed blob](media/compaction_range_distribution.png)

![Size histogram for flushed blobs](media/blob_sizes_no_split.png)

So the majority of flushed blobs are contained in one compaction range, and an absolute majority of blobs contain 12 or fewer blocks.

We can go further and start splitting blobs if they span only a few ranges and the original blob is already very small. For example, we can split only when the original blob and every resulting sub-blob are either all larger than 64 KiB or all smaller than 64 KiB. The 64 KiB value comes from the [BlobStorage threshold for huge blobs on SSD](https://github.com/ydb-platform/ydb/blob/645ff364ef1cac6489b2282df24f7d7b4997d25e/ydb/core/blobstorage/vdisk/common/vdisk_config.cpp#L143). All blobs smaller than this threshold are equally bad for BlobStorage, so we can use this constant as the threshold for our split. Additionally, I measured the distribution of final blob sizes if we split blobs that span two or three compaction ranges.

#### Split for 2 compaction ranges:

![Size histogram for flushed blobs](media/blob_sizes_split2.png)

#### Split for 3 compaction ranges:

![Size histogram for flushed blobs](media/blob_sizes_split3.png)

So the number of blobs larger than 64 KiB does not change much. Only blobs that contain 4 or 6 blocks disappear. With such a split, almost 90 percent of blobs are small enough to lie entirely in one compaction range, while blobs larger than 64 KiB stay roughly the same.

## Performance

I ran some tests of this optimization; here are the results:

I brought the two disks to the same compaction score and started fio:
`fio --rw=randwrite --name=randwrite --filename=/dev/disk/by-id/virtio-blockmask --direct=1 --bs=4k --ioengine=libaio --iodepth=32 --group_reporting --size=512GB --rate_iops=4500`

Compaction score:

<img width="2933" height="487" alt="Image" src="https://github.com/user-attachments/assets/7395fcf4-0249-49b4-a9b9-f5914a50ac40" />

Compaction time percentiles:

p50:

<img width="2933" height="487" alt="Image" src="https://github.com/user-attachments/assets/78e0741b-d9bc-4cf7-8479-034f2de31edd" />

p90:

<img width="2933" height="487" alt="Image" src="https://github.com/user-attachments/assets/cea05ab4-3778-4f22-b0d4-770c9479ae8f" />

p99:

<img width="2933" height="487" alt="Image" src="https://github.com/user-attachments/assets/ea7a2bf8-63c9-47d6-acba-348263565bc4" />

Execution CPU time of the compaction transaction:

<img width="2933" height="487" alt="Image" src="https://github.com/user-attachments/assets/3c57204c-da74-48a4-9a3b-2ad7c7da348f" />

We can see a strong performance boost in compaction.
