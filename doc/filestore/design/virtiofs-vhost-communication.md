# Guest virtiofs <-> NBS filestore-vhost Communication

## Introduction

`virtiofs` transports FUSE filesystem operations between a guest VM and a host
userspace daemon. In the guest kernel, filesystem syscalls are translated into
FUSE requests and placed into virtio rings (virtqueues) as descriptor chains in
shared memory. On the host side, QEMU and the `vhost-user` protocol connect
those queues to `filestore-vhost`, which reads requests, dispatches them to the
filesystem implementation, and writes completions back to the guest.

In this document:
- **frontend queues** are guest-visible virtiofs queues (one hiprio queue plus
  request queues);
- **backend queues** are host-side `filestore-vhost` worker/request queues used
  to poll, dispatch, and complete requests.

## End-to-end write request flow

This section covers regular file write requests (`write`/`write_buf`) from a
guest VM to `filestore-vhost` and dispatch to service.

### Step-by-step flow

1. Guest userspace issues `write(2)` on a virtiofs mount. Linux enters FUSE
   write path (`fuse_file_write_iter` and related helpers):
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/file.c#L1640
2. FUSE request is queued into virtiofs transport via
   `virtio_fs_wake_pending_and_unlock`, then enqueued to virtqueue by
   `virtio_fs_enqueue_req`:
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L1224
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L1121
3. On host, vhost-user backend receives queue events. In NBS virtio bridge,
   backend loop runs `vhd_run_queue`, `vhd_dequeue_request`, and
   `process_request`:
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L535
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L241
4. Request is dispatched into FUSE low-level ops in `loop.cpp`. `ops.write` and
   `ops.write_buf` are wired in `TFileSystemLoop::InitOps`, both routed through
   `TFileSystemLoop::Call(...)`:
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L1502
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L1641
5. `IFileSystem::Write`/`WriteBuf` implementation (`TFileSystem`) builds
   `NProto::TWriteDataRequest` and runs `DoWrite(...)`:
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/fs_impl_data.cpp#L452
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/fs_impl_data.cpp#L489
6. `DoWrite(...)` either goes through write-back cache or directly calls
   `Session->WriteData(...)`. Session layer then sends request through
   `ExecuteRequestWithSession(...)` to underlying service client:
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/fs_impl_data.cpp#L522
7. Response is returned to FUSE reply helpers and then to virtio side. Host
   completion is sent via `virtio_send_msg(...)`:
   https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L555
8. Guest completion path is handled in virtiofs done workers
   (`virtio_fs_vq_done`, `virtio_fs_requests_done_work`,
   `virtio_fs_request_complete`):
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L653
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L609
   https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L558

## Guest critical-path locks (frontend/guest)

Briefly, guest write path uses queue locks, inode locks, and a background
flow-control lock.

- Queue/transport locks:
  - `fiq->lock` protects input request queueing (`pending` list) in FUSE core:
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/fuse_i.h#L439
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dev.c#L225
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dev.c#L410
  - `fsvq->lock` protects virtqueue state (`connected`, `queued_reqs`,
    `end_reqs`, in-flight counters) during enqueue/completion:
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L1121
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L609
  - `fpq->lock` protects processing-list transitions in fuse pqueue:
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/fuse_i.h#L478
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/virtio_fs.c#L558

- Background control:
  - `fc->bg_lock` protects background throttling state (`num_background`,
    `active_background`, `bg_queue`, `blocked`):
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/fuse_i.h#L621
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dev.c#L524
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dev.c#L280
  - It is adjacent to the write datapath under async/load pressure, but is not
    the normal synchronous fast write path lock.

- Inode locks on write path:
  - VFS inode lock (`inode_lock` / `inode_lock_shared`) is used in cache/direct
    write paths to serialize write/truncate-sensitive operations:
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/file.c#L1328
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/file.c#L1590
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/file.c#L1592
  - FUSE per-inode spinlock `fi->lock` protects FUSE inode write state and
    attr/size updates (for example `fuse_write_update_attr`):
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/fuse_i.h#L116
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/fuse_i.h#L166
    https://github.com/torvalds/linux/blob/v6.8/fs/fuse/file.c#L1095

## Backend queues and locks (filestore-vhost)

Backend processing uses one request queue per backend worker thread (fuse thread). Each
thread runs the queue loop, dequeues requests, processes them, and sends
responses back to guest buffers.

- Transport queue (backend vhost queue):
  - Queue loop and dequeue/process path:
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L529
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L544
  - Response path:
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/vhost/fuse_virtio.c#L555
  - Backend queue threads (fuse thread) are started per backend queue:
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L603
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L635

- Completion tracking lock (`TCompletionQueue`):
  - Per-bucket `TAdaptiveLock` protects in-flight request map/counters during
    enqueue and completion:
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L107
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L152
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L174
  - Requests enter this path from loop dispatch and are completed from reply
    helpers:
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/loop.cpp#L1457
    https://github.com/ydb-platform/nbs/blob/1e0bfbdbfd51509f9fbe95931b726ce24b42d522/cloud/filestore/libs/vfs_fuse/fs_impl.h#L497
