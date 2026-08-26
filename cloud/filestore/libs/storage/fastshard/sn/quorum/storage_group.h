#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

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
 * Returns an IStorageGroup which mirrors each write into all storage nodes and
 * reads from one of the nodes selecting it in a round-robin manner. The
 * implementation is naive in the sense that it does no crash recovery and is
 * basically a happy-path implementation intended for tests and prototyping. And
 * there's also no real m/n write / k/n read quorum here - it's just always
 * n/n for writes, 1/n for reads.
 *
 * @return - The constructed group.
 */
IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<TStorageDevice> devices);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
