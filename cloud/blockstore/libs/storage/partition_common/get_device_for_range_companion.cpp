#include "get_device_for_range_companion.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/storage/core/libs/actors/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TGetDeviceForRangeCompanion::TGetDeviceForRangeCompanion(
        EAllowedOperation allowedOperation)
    : AllowedOperation(allowedOperation)
{}

TGetDeviceForRangeCompanion::TGetDeviceForRangeCompanion(
        EAllowedOperation allowedOperation,
        TNonreplicatedPartitionConfigPtr partConfig)
    : AllowedOperation(allowedOperation)
    , PartConfig(std::move(partConfig))
{}

void TGetDeviceForRangeCompanion::SetDelegate(NActors::TActorId delegate)
{
    Delegate = delegate;
}

void TGetDeviceForRangeCompanion::HandleGetDeviceForRange(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    using EPurpose =
        TEvNonreplPartitionPrivate::TGetDeviceForRangeRequest::EPurpose;

    const auto* msg = ev->Get();

    bool operationAllowed = false;
    switch (AllowedOperation) {
        case EAllowedOperation::Read:
            operationAllowed = msg->Purpose == EPurpose::ForReading;
            break;
        case EAllowedOperation::ReadWrite:
            operationAllowed = true;
            break;
    }
    if (!operationAllowed) {
        ReplyCanNotUseDirectCopy(ev, ctx);
        return;
    }

    if (Delegate) {
        ForwardMessageToActor(ev, ctx, Delegate);
        return;
    }

    if (!PartConfig) {
        auto response = std::make_unique<
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>(MakeError(
            E_INVALID_STATE,
            "GetDeviceForRange companion not initialized"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto requests = PartConfig->ToDeviceRequests(msg->BlockRange);
    if (requests.size() != 1) {
        ReplyCanNotUseDirectCopy(ev, ctx);
        return;
    }

    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>();

    response->Device = requests[0].Device;
    response->DeviceBlockRange = requests[0].DeviceBlockRange;

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TGetDeviceForRangeCompanion::RejectGetDeviceForRange(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>(
        MakeError(E_REJECTED, "GetDeviceForRange request rejected"));
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TGetDeviceForRangeCompanion::ReplyCanNotUseDirectCopy(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    const auto* msg = ev->Get();

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>(MakeError(
            E_ABORTED,
            TStringBuilder() << "Can't use direct range copying for "
                             << DescribeRange(msg->BlockRange))));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
