#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// A companion for processing EvGetDeviceForRangeRequest requests. Finds how
// many requests needs for processing the range. If only one request is
// needed, then everything is fine, request can be executed in direct
// DiskAgent-to-DiskAgent mode.
// Performs the work himself, if the PartConfig is set, or delegates it to
// another actor if it set via SetDelegate method. If neither is done, it
// responds to requests with an error.
class TGetDeviceForRangeCompanion
{
public:
    enum class EAllowedOperation
    {
        None,       // Reply with an error to all requests
        Read,       // Reply with an error to requests intended for writing
        ReadWrite   // Requests with any purpose are allowed
    };

private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;

    EAllowedOperation AllowedOperation;
    NActors::TActorId Delegate;

public:
    explicit TGetDeviceForRangeCompanion(EAllowedOperation allowedOperation);

    TGetDeviceForRangeCompanion(
        EAllowedOperation allowedOperation,
        TStorageConfigPtr config,
        TNonreplicatedPartitionConfigPtr partConfig);

    void SetAllowedOperation(EAllowedOperation allowedOperation);
    void SetDelegate(NActors::TActorId delegate);

    void HandleGetDeviceForRange(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
    void RejectGetDeviceForRange(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
    void ReplyCanNotUseDirectCopy(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;

private:
    [[nodiscard]] TDuration GetMinRequestTimeout() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
