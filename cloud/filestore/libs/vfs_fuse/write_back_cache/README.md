Write-Back cache is intended to speed up client write requests via:

1. Latency reduction: clients get immediate responses for write requests
while their requests are actually written in background.

2. Rate reduction for write requests to a filestore server: overlapping and
adjacent write requests can be combined into a single write request.


# Requirements

1. Clients should observe the effects of the cached write requests as they
have been actually written.

2. Clients should not observe any state that cannot occur when write-back
caching is not used.

3. No write requests should be lost even when the service restarts.


# General information

## File-backed ring buffer

All cached write requests are serialized into a queue — a ring buffer that
is backed by a file via memory mapping. The file is stored in tmpfs
(physically — in the memory of the host where the guest VM is running).
It ensures that the requests are not lost when the service restarts.

The observed state is calculated as the actually stored state after atomically
applying the cached requests on top of each other in the order they were stored
in the queue.

The cached requests are removed from the queue when it is guaranteed that
they do not have effect on the observed state anymore.


## Read strategy

Read requests should return any valid state that may be observed between the
moment when read request has been received and the moment when read response has
been completed.

Read requests are completed immediately if the requested read range is
сompletely covered by the cached write requests. Otherwise a request to the
filestore is performed and cached requests are applied on top of the response.


## Write strategy

Write requests are stored in the ring buffer and become cached if the ring
buffer is capable to store them. At the same moment, a response is generated
for each request — clients see their requests as completed.

If the ring buffer has reached its capacity, write requests are put into
a pending queue. They are moved to the ring buffer when it is ready to accept
requests. The requests do not affect the observed state while they are waiting
in the pending queue.


## Flush strategy

Flush operation updates the actual state by applying the cached write requests
from the ring buffer to it. Write request to adjacent and overlapping regions
can be combined to a single write request to the filestore server.

After having completed write requests to the filestore server, the cached write
requests are marked as flushed and removed from the ring buffer.

Flush operation can be applied to the cached requests for a specific handle or
to all cached requests in the ring buffer (FlushAll).


# Implementation details

## Read

A client read request returns the observed state at the moment the read request
has been received:

1. The response buffer is filled by data from the cached write requests.

2. The remaining data is queried from the filestore server.

It may happen that a series of write requests is flushed while a read query is
executing. In this case, the read query will return newer actual state but the
response buffer is already filled by older data. This breaks the requirement
that clients should not observe any state that cannot occur when write-back
caching is not used.

A session sequencer is used to prevent this situation. It internally acts as
a read-write lock that blocks reads to ranges that are already being written
and blocks writes to ranges that are already being read or written.


## Write

The client requests waiting in the pending queue are moved to the ring buffer
after completion of FlushAll operation.


## Flush

FlushAll operation is triggered when the ring buffer is incapable to store
client write requests or a certain amount of time is passed after the last
FlushAll oparation has completed.


# Concerns

## Capacity

1. The capacity of the ring buffer should be greater than the maximal client
write request size otherwise big requests will hang.

2. Flush operation may generate concatenated write requests to the filestore
server that have size greater than the maximal allowed size.

## Hanging requests


# Formalities

## Definitions

`S[k]` - state;
`W[k]` — write requests.
`S[k + 1] = S[k] op W[k]`.

Write requests are non-commutative (arbitrary reordering is not allowed):
`S[k] op W[k] op W[k + 1] != S[k] op W[k + 1] op W[k]`

Write requests are associative (can be grouped):
`(S[k] op W[k]) op W[k + 1] = S[k] op (W[k] op W[k + 1])`

Write requests are idempotent:
`W[k] op W[k] = W[k]`

Extended idempotency:
`W[k] op (...) op W[k] = (...) op W[k]`


## Write-back caching

`S[p]` — actual state stored in the filestore;
`W[p] ... W[q - 1]` — cached write requests;
`S[q]` — current observed state.


## Read operation

1. Set `qr = q`.

2. For each `k` in `[p, qr)` intersect `W[k]` with the read range and combine
them into a single write request `Wpq`.

3. Send read request to the filestore.

(here other write and flush operations may take place)

4. Receive some actual state `S` from the filestore.

5. Return `(S op Wpq)` as the result of read operation.

The result is valid if `S[p] <= S <= S[qr]`.


## Write operation

1. Add `W[q]` to a queue.

2. Increment `q`.


## Flush operation

1. Store `qf = q`.

2. Take the write requests from the queue and combine then into one or several
write operations:

`W[p] op W[p + 1] op ... op W[qf - 1] = W'[0] op W'[1] op ... op W'[n - 1]`

3. Send write requests to the filestore.

(here other read and write operations may take place)

4. Wait until all the request are written.

5. Set `p = qf`.


## Inconsistencies

1. Two simultaneously executing flush operation may reorder write operations.

Solution: forbid this, flush operations should be sequenced.

2. Read operation may receive `S > S[qr]` if there were new writes that were
flushed.

Solution: each read operation sets a barrier to a range with generation
number `qr`. Flush operation should wait if `qf > qr`.


## Session sequencer API proposal:

```
SetBarrier(handle, offset, length, gen_no);
ResetBarrier(handle, offset, length, gen_no);
WriteData(request, gen_no);
```
