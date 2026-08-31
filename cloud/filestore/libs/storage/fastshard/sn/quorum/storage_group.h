#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TPageGroupRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
    ui64 PageSize = 0;
};

struct TPageGroup
{
    ui64 FirstPageNo = 0;
    TVector<TBuffer> Content;
};

/**
 * Storage group iface. Storage groups are supposed to provide some extra
 * non-functional features on top of multiple storage devices - like redundancy
 * or hedged requests. Storage groups are also responsible for locking the
 * devices, replaying the tail of the log, maintaining log-sequence-number and
 * in general for the relative consistency of the devices.
 *
 * The interface is synchronous and is supposed to be used from a fiber.
 */
struct IStorageGroup
{
    virtual ~IStorageGroup() = default;

    virtual NProto::TError AcquireDevices() = 0;
    virtual NProto::TError ReleaseDevices() = 0;
    virtual NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) = 0;
    virtual NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) = 0;
};

using IStorageGroupPtr = std::shared_ptr<IStorageGroup>;

struct TStorageDevice
{
    IStorageNodePtr Node;
    TString DeviceUUID;
};

/**
 * Retry policy for the requests which a storage group sends to its storage
 * nodes. Retriable errors (see GetErrorKind) are retried with a backoff which
 * grows by BackoffIncrement after each failed attempt of the same request:
 * BackoffIncrement after the first error, 2 * BackoffIncrement after the
 * second one, etc. When the time passed since the start of the first attempt
 * till the last error reaches TotalTimeout, the last error is propagated to
 * the caller.
 */
struct TStorageGroupRetryPolicy
{
    TDuration TotalTimeout = TDuration::Minutes(5);
    TDuration BackoffIncrement = TDuration::MilliSeconds(500);
};

/**
 * Returns an IStorageGroup which mirrors each write into all storage nodes and
 * reads from one of the nodes selecting it in a round-robin manner. The
 * implementation is naive in the sense that it does no crash recovery and is
 * basically a happy-path implementation intended for tests and prototyping. And
 * there's also no real m/n write / k/n read quorum here - it's just always
 * n/n for writes, 1/n for reads.
 *
 * @param devices - Storage devices to mirror the data across.
 * @param retryPolicy - Retry policy for storage node requests.
 * @param timer - Time source for the retry deadline checks and backoff
 *                sleeps. Production callers should pass the timer returned
 *                by CreateFiberTimer(). Tests can pass TTestTimer to make
 *                retries deterministic.
 * @return - The constructed group.
 */
IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<TStorageDevice> devices,
    TStorageGroupRetryPolicy retryPolicy,
    ITimerPtr timer);

/**
 * Returns an ITimer which sleeps by suspending the calling silk fiber, so
 * backoffs do not block the fiber scheduler threads.
 *
 * @return - The constructed timer.
 */
ITimerPtr CreateFiberTimer();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
