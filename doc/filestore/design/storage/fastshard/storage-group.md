# Storage group

A storage group is N storage devices treated as one replicated unit. The
shard's page store addresses a group, never an individual device. The
interface is `IStorageGroup` (`sn/quorum/storage_group.h`): `AcquireDevices`,
`ReleaseDevices`, `WriteLogRecord`, `ReadPages` - synchronous, called from
silk fibers. In the production implementation we'll also need the methods for:
* advancing LSN low watermark (below which the journal can be checkpointed and
  trimmed)
* locking the storage group (to ensure that an old instance of the same shard
  dies upon access attempt and doesn't corrupt anything)
* reading the tail of the journal (to do recovery upon restart)

## Responsibilities

* **Data redundancy** - every log record is replicated across the group's
  devices. Write quorum m/n, read quorum k/n.
* **Device locking** - the group acquires its devices before serving
  traffic; the lease carries the writer's `Generation`, which fences stale
  writers. *Missing in the prototype.*
* **Recovery** - after a crash the group replays the tail of the log,
  bringing all devices to a consistent state before accepting new writes.
  *Missing in the prototype.*
* **Retries and ordering** - the group retries per-device failures and
  guarantees that the LSN moves forward monotonically and without gaps.
  *Missing in the prototype.*

## LSN advancement

Every `WriteLogRecord` carries an LSN. The group is the layer that
guarantees the LSN sequence observed by every device is monotone and
gapless: a device that has applied LSN X has applied every record below X.
This property is what makes tail replay after a crash well-defined: the
recovery procedure finds the highest LSN present on a quorum of devices and
completes or discards records above it. If an operation gets cancelled the shard
is still responsible for notifying the group of the LSN of this operation. It
doesn't need to happen as a separate event - these cancelled LSNs can be
piggybacked with the next write-log-record message.

The LSN low watermark - the boundary below which journal records may be
applied to their final locations - is moved by the shard and forwarded by
the group to every device (see [storage-node.md](storage-node.md)).

## Prototype

`CreateNaiveMirroredStorageGroup` is a happy-path implementation:

* writes are mirrored to all devices, n/n - no write quorum;
* reads go to one device selected round-robin, 1/n;
* no locking, no recovery, no LSN enforcement, no retries.

## Diagram

![fastshard_storage_group](../../../excalidraw/fastshard_storage_group.svg)
