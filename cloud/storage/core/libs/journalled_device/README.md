# journalled_device

A write-ahead journal in front of a block device, plus the pieces it is built
from. The entry point is `CreateJournalledDeviceV2`.

The problem it solves: a writer wants its writes acknowledged quickly and
durably, and wants to be able to re-read what it has written after a restart,
but the final location of the data is a device that may be slow, remote, or
shared. So the writes go into a journal first, are acknowledged from there, and
are copied to their final location in the background once the writer says it no
longer needs them kept.

## Interfaces

`IDevice` (`device.h`) is the raw device: `ReadPages` and `WritePages`, both
addressing whole pages by number. `CreateInMemoryDevice` is the test
implementation.

`IJournalledDevice` (`journalled_device.h`) is what the stack exposes:

| method | meaning |
| --- | --- |
| `WriteLogRecord` | write a record - a set of page groups tagged with an `Lsn` and the `PrevLsn` it chains from |
| `ReadPages` | read pages, newest content wins, whether it is still in the journal or already on the device |
| `ReadJournalTail` | the records the journal still holds, in chain order |
| `AdvanceLsnLowWatermark` | the writer no longer needs the records below this lsn - they may be applied and dropped |

There are two implementations. `CreateJournalledDevice` (`journalled_device.cpp`)
is the v1 pass-through: it validates the lsn order and forwards everything to
the device, with no journal at all; `ReadJournalTail` and
`AdvanceLsnLowWatermark` return `E_NOT_IMPLEMENTED`.
`CreateJournalledDeviceV2` is the real thing and is what the rest of this
document is about.

## The stack

```
                  IJournalledDevice  (journalled_device_v2.cpp)
                  - splits reads between the journal and the data device
                  - runs the background flush cycle
                           |
              +------------+-----------------------------+
              |                                          |
          IJournal  (journal.cpp)                    IDevice
          - record chain, page index                 "data device":
          - restore, tail, watermark                 where the pages
              |                                      finally live
      +-------+---------------------+
      |                             |
IKeyBufferStore              IDevicePageStore
(key_buffer_store.cpp)       (device_page_store.cpp)
"log meta": one entry        "log data": page allocator
per record, keyed by         over a device, holds the
PrevLsn                      record payloads
      |                             |
   IDevice                       IDevice
```

Three devices, then: one for the journal metadata, one for the journal data,
one for the data itself. They can be three areas of one physical device or
three separate ones - the stack only cares that the three page ranges do not
overlap.

Building it:

```c++
auto metaStore = CreateDeviceKeyBufferStore(logMetaDevice, pageCount, pageSize);
auto pageStore = CreateDevicePageStore(logDataDevice, pageCount, pageSize);
auto journal = CreateJournal(logging, executor, metaStore, pageStore);
auto device = CreateJournalledDeviceV2(
    logging, executor, journal, dataDevice, deviceUUID, backgroundClientId);

device->Start();   // restores the journal and starts the flush cycle
```

## Components

### `log_record.h` - what a record is

```c++
struct TLogRecord
{
    ui64 Lsn;
    ui64 PrevLsn;
    TVector<TPageMapping> PageMappings;   // pageNo -> where it sits in the log
    TPromise<TWriteLogRecordResponse> Promise;
};
```

A record says "these pages now hold this content", and chains to its
predecessor through `PrevLsn`. The lsns are the writer's, the journal never
invents them; it only requires `PrevLsn < Lsn`. The `PageMappings` are filled in
by the journal once it knows where in the log the payload landed, and they are
what gets serialized into the metadata store - the payload itself stays on the
log data device.

The file also carries the (de)serialization of a record and of the journal
metadata (`TJournalMetadata`, currently just `LastAckedLsn`).

### `log_chain.h` - the in-flight records

`TLogRecordChain` holds the records keyed by **`PrevLsn`**, which is what makes
out-of-order arrival work: a record can be inserted before the one it chains
from has shown up, leaving a gap. The records that follow each other unbroken
from `LastErasedLsn` form the *chained run*, ending at `LastChainedLsn`; only
records in that run can be served, flushed or erased.

A record becomes `Ready` once it is durable (payload and metadata both
written). `Insert` rejects a record that overlaps one already held or that ends
at or below the erased watermark, and hands back the held record for an exact
duplicate so the caller can wait on the same promise.

### `log_index.h` - where a page currently lives

`TLogPageIndex` maps a page number to the log location holding its newest
content, along with the lsn that wrote it. `TryApplyNext` applies a record only
if it continues the chain from `LastIndexedLsn`, splitting and overwriting the
mappings it covers; this is what makes a record visible to readers.
`Lookup(ranges, afterLsn)` returns the mappings written *after* `afterLsn`,
clipped to the requested ranges - pages that were already flushed are left out
so the reader falls through to the data device.

### `lsn_barrier.h` - keeping the ground still under a reader

`TLsnBarrier` is a monotonically advancing lsn with reference-counted guards.
`Acquire` pins the current value; `GetBarrierLsn` returns the lowest pinned
value, or the current one when nothing is pinned. Two of them are used:

- the journal's `FlushedLsnBarrier` - how far the journal has been flushed.
  A read, a tail read and the flush loop each pin it, so cleanup cannot free
  pages out from under them.
- the device's `IndexedLsnBarrier` - how far the journal has been indexed.
  A read pins it, and the flush cycle refuses to flush past the pinned value,
  so a reader that took its journal pages at one lsn cannot then see device
  pages from a later one.

### `device_page_store.h` - the log data allocator

`TDevicePageStore` owns the page allocation of a device: `Allocate`,
`AllocateAt` (used by restore to re-claim the pages of the restored records),
`Free`, and page-granular `Read`/`Write`. Allocation is guarded by a lock and
may be called from anywhere; the data methods validate against the allocation
state under that lock but issue the device request outside of it, so the caller
must not free pages it is still reading or writing. In `Trusted` mode the
validation is skipped everywhere but `AllocateAt`.

### `key_buffer_store.h` - the log metadata store

A small persistent map of `ui64` key to `TBuffer`: `Restore` (read everything
back), `Write` (upsert), `EraseBelow` (drop every key strictly below a bound).
Two implementations: `CreateInMemoryKeyBufferStore` for tests, and
`CreateDeviceKeyBufferStore`, which lays the map out on a device:

- pages 0 and 1 are **superblock slots**, written alternately: magic, seq,
  `ErasedBelowKey`, version, crc32c. The intact slot with the highest seq wins,
  so an interrupted erase leaves the previous bound in place.
- every other page is an **entry page**: a 48-byte header (magic, seq, key,
  payload size, version, page index, page count, crc32c over the header and the
  chunk) followed by a chunk of the value. A value larger than one chunk spans
  several pages, which need not be contiguous - the pages come from a
  `TDevicePageStore` over the same device.
- `Restore` scans every page, groups the intact ones by `(key, seq)`, keeps the
  newest *complete* candidate of each key, and drops the keys below the
  superblock's `ErasedBelowKey`. A torn write is therefore simply an incomplete
  candidate that loses to the previous seq.
- a `Write` that supersedes an entry frees the pages of the older one only
  after the new one is durable, so a crash mid-write leaves the old value
  readable.

### `journal.h` - the journal itself

`TJournal` ties the chain, the index and the two stores together. All of its
methods run on the coroutine `TExecutor` - they wait on the store futures with
`Executor->WaitFor`, so they must be called from an executor thread.

## The paths

Both of them require the page ranges of a single request to be disjoint. The
journal checks this before touching anything and answers `E_ARGUMENT`: a page
covered by two ranges of one request has no single answer - on a read it
would be looked up twice and taken from the merged response twice, on a write
it would be mapped twice, leaving the winner to the order the groups happen
to be in and handing the tail two groups claiming the same page. Ranges
covering no pages are ignored by the check.

### Write

1. The page groups must not intersect (`E_ARGUMENT`).
2. `Lsn` must not be the reserved metadata key (`Max<ui64>`); a record at or
   below `LastIndexedLsn` is answered `S_ALREADY`.
3. The record is inserted into the chain. A duplicate returns the held
   record's future; an overlapping one is rejected with `E_INVALID_STATE`.
4. The payload pages are allocated in the log data store (`E_REJECTED` when
   the journal is full) and written.
5. The page mappings are now known, so the record is serialized and written to
   the metadata store **under the key `PrevLsn`**.
6. The record is marked ready, and the journal walks forward from it applying
   every ready record that continues the chain to the page index, completing
   each one's promise as it goes.

A record that does not continue the chain yet stays pending at step 6 - its
promise is completed when the missing predecessor arrives. If anything fails,
the record is removed from the chain and its pages are freed.

### Read

`TJournal::Read` pins the flushed barrier, looks the ranges up in the page
index, reads the mapped pages out of the log data store, and returns them as
page groups - only the pages the journal actually has.

`TJournalledDeviceV2::DoReadPages` pins the indexed barrier, asks the journal, computes the gaps (`MakeMissingRequest`), reads those from the
data device, and stitches the two responses together in the shape of the
request (`MergeResponses`). A page missing from both is an `E_INVALID_STATE`.

### Tail

`ReadJournalTail` pins the flushed barrier at no less than the requested lsn,
starts at the later of that and the acked lsn, and walks the ready run forward,
reading each record's payload back out of the log data store. This is how a
writer that restarts finds out what the journal already holds.

### Watermark

`AdvanceLsnLowWatermark` refuses a watermark above `LastIndexedLsn`
(`E_ARGUMENT`), refuses to run concurrently with itself (`E_REJECTED`), answers
`S_ALREADY` for a watermark that is not new, and otherwise persists
`TJournalMetadata` under the metadata key before moving `LastAckedLsn`. Records
are flushed only up to this value: until the writer acks them, they stay in the
journal and the data device is left untouched.

### Flush cycle

Started by `Start` and rescheduled every `IdleFlushDelay` (100 ms):

1. `GetRecordToFlush(maxAllowedLsn)` returns the next record after the flushed
   barrier, provided its lsn is at or below both `maxAllowedLsn` (the indexed
   barrier, i.e. what in-flight readers have seen) and `LastAckedLsn`.
2. The record's page groups are written to the data device at their real page
   numbers, and `MarkRecordAsFlushed` advances the flushed barrier. There is no
   client request behind a flush, so the write carries the `deviceUUID` and
   `backgroundClientId` the device was built with - a device refuses a write
   with neither. A record that maps no pages is a chain link and nothing more,
   so it is marked flushed without a device request at all.
3. When there is nothing left to flush, `CleanupFlushedRecords` erases the
   metadata entries below the barrier (`EraseBelow`, keyed by `PrevLsn`), drops
   the index mappings at or below it, removes the records from the chain and
   frees their log pages. Records stranded below the new watermark - ready, but
   chaining from a record that can never arrive - have their promises failed
   with `E_INVALID_STATE`.

`Stop` sets a flag the loop checks between records and waits for it to finish.

### Restore

`Start` runs `Journal->Restore` on the executor:

1. every key/buffer pair is read back from the metadata store and sorted by key
   (`PrevLsn`);
2. the entry under the metadata key, if any, gives `LastAckedLsn`;
3. the records are deserialized in chain order; the first one initializes the
   chain, index and flushed barrier to `min(PrevLsn, LastAckedLsn)`;
4. each record's pages are re-claimed in a freshly built page store with
   `AllocateAt`, so the allocation state is derived from the records - pages
   orphaned by a crash mid-write simply come back as free;
5. each record is inserted, marked ready and applied to the index.

It returns `LastIndexedLsn`, which the device uses to initialize the indexed
barrier. A log that ends below the acked lsn is an `E_INVALID_STATE`.

## Threading

`TJournalledDeviceV2` funnels every public call through the coroutine
`TExecutor`, so both it and the journal below it run on executor threads and
may block on futures there. The stores are free-threaded: their state is under
a lock and the device requests are issued outside of it. The chain, the index
and the barriers each guard themselves with a spinlock.

## Errors worth knowing

| code | when |
| --- | --- |
| `S_ALREADY` | the record is already indexed, or the watermark is not new |
| `E_REJECTED` | the journal is out of pages, or an erase/advance is already in progress |
| `E_ARGUMENT` | intersecting page ranges in one request, a watermark above the indexed lsn, a key below the erased bound |
| `E_INVALID_STATE` | a record that cannot join the chain, a store used before restore, corrupt metadata |

## Tests

| file | what it covers |
| --- | --- |
| `device_ut.cpp` | the in-memory device |
| `device_page_store_ut.cpp` | allocation, page validation, the two modes |
| `key_buffer_store_ut.cpp` | both stores, including the on-device format, torn writes and restore |
| `log_record_ut.cpp` | (de)serialization |
| `log_chain_ut.cpp`, `log_index_ut.cpp`, `lsn_barrier_ut.cpp` | the standalone data structures |
| `journal_ut.cpp` | the journal over a real page store and a fault-injecting metadata store |
| `journalled_device_v2_ut.cpp` | the device over a mocked journal - read splitting, merging, the flush cycle |
| `journalled_device_v2_stack_ut.cpp` | the whole stack over three in-memory devices, including restarts |
