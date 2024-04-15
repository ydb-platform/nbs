#pragma once

#if !defined(BLOCKSTORE_INCLUDE_REQUEST_HELPERS_INL)
#   error "this file should not be included directly"
#endif

#include <cloud/storage/core/libs/common/random.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {
    template <typename T>
    struct TRequest {};

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    template <>                                                                \
    struct TRequest<NProto::T##name##Request>                                  \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

}   // namespace NImpl

template <typename T>
constexpr EBlockStoreRequest GetBlockStoreRequest()
{
    return NImpl::TRequest<T>::Request;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsReadRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::WriteBlocksLocal:
            return true;

        default:
            return false;
    }
}

constexpr bool IsReadWriteRequest(EBlockStoreRequest requestType)
{
    return IsReadRequest(requestType) || IsWriteRequest(requestType);
}

constexpr bool IsNonLocalReadWriteRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
            return true;

        default:
            return false;
    }
}

constexpr EBlockStoreRequest TranslateLocalRequestType(
    EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocksLocal:
            return EBlockStoreRequest::ReadBlocks;
        case EBlockStoreRequest::WriteBlocksLocal:
            return EBlockStoreRequest::WriteBlocks;

        default:
            return requestType;
    }
}

constexpr bool IsControlRequest(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::Ping:
        case EBlockStoreRequest::MountVolume:
        case EBlockStoreRequest::UnmountVolume:
        case EBlockStoreRequest::UploadClientMetrics:
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
        case EBlockStoreRequest::WriteBlocksLocal:
        case EBlockStoreRequest::QueryAvailableStorage:
            return false;
        case EBlockStoreRequest::CreateVolume:
        case EBlockStoreRequest::DestroyVolume:
        case EBlockStoreRequest::ResizeVolume:
        case EBlockStoreRequest::StatVolume:
        case EBlockStoreRequest::AssignVolume:
        case EBlockStoreRequest::CreateCheckpoint:
        case EBlockStoreRequest::DeleteCheckpoint:
        case EBlockStoreRequest::AlterVolume:
        case EBlockStoreRequest::GetChangedBlocks:
        case EBlockStoreRequest::GetCheckpointStatus:
        case EBlockStoreRequest::DescribeVolume:
        case EBlockStoreRequest::ListVolumes:
        case EBlockStoreRequest::DiscoverInstances:
        case EBlockStoreRequest::ExecuteAction:
        case EBlockStoreRequest::DescribeVolumeModel:
        case EBlockStoreRequest::UpdateDiskRegistryConfig:
        case EBlockStoreRequest::DescribeDiskRegistryConfig:
        case EBlockStoreRequest::CreatePlacementGroup:
        case EBlockStoreRequest::DestroyPlacementGroup:
        case EBlockStoreRequest::AlterPlacementGroupMembership:
        case EBlockStoreRequest::DescribePlacementGroup:
        case EBlockStoreRequest::ListPlacementGroups:
        case EBlockStoreRequest::CmsAction:
        case EBlockStoreRequest::CreateVolumeFromDevice:
        case EBlockStoreRequest::ResumeDevice:
        case EBlockStoreRequest::StartEndpoint:
        case EBlockStoreRequest::StopEndpoint:
        case EBlockStoreRequest::ListEndpoints:
        case EBlockStoreRequest::KickEndpoint:
        case EBlockStoreRequest::ListKeyrings:
        case EBlockStoreRequest::DescribeEndpoint:
        case EBlockStoreRequest::RefreshEndpoint:
            return true;
        case EBlockStoreRequest::MAX:
            Y_DEBUG_ABORT_UNLESS(false);
            break;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsDataChannel(NProto::ERequestSource source)
{
    switch (source) {
        case NProto::SOURCE_TCP_DATA_CHANNEL:
        case NProto::SOURCE_FD_DATA_CHANNEL:
            return true;

        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool IsThrottlingDisabled(const T& request)
{
    return HasProtoFlag(request.GetMountFlags(), NProto::MF_THROTTLING_DISABLED);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
consteval bool ShouldBeThrottled()
{
    return IsReadWriteRequest(GetBlockStoreRequest<T>());
}

}   // namespace NCloud::NBlockStore
