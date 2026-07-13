#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * A dumb file-backed IStorageNode.
 *
 * Semantics:
 *   - AcquireDevices and ReleaseDevices are stubbed and return S_OK.
 *   - WriteLogRecord writes every page in every TDevicePageGroup
 *     straight to `path` at offset (FirstPageNo + i) * pageSize, where
 *     pageSize is the size of the first Content entry of that group.
 *     No journal, no ordering, no atomicity.
 *   - ReadPages reads PageCount * PageSize bytes at
 *     (FirstPageNo * PageSize) for each TDevicePageGroupRef and slices
 *     the buffer into one Content entry per page.
 *
 * The file at `path` must already exist and be large enough for the
 * requested I/O.
 *
 * Callers must run from a silk fiber; I/O is submitted through
 * silk::FiberScheduler (io_uring) and suspends the calling fiber until
 * completion.
 *
 * @param path - Filesystem path opened OpenExisting|RdWr for the whole
 *   lifetime of the returned node.
 *
 * @return - Shared owner of the storage node instance.
 */
IStorageNodePtr CreateNaiveFileStorageNode(TString path);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
