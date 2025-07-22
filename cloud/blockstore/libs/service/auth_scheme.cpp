#include "auth_scheme.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ToString(EPermission permission)
{
    switch (permission) {
        case EPermission::Get:
            return "nbsInternal.disks.get";
        case EPermission::List:
            return "nbsInternal.disks.list";
        case EPermission::Create:
            return "nbsInternal.disks.create";
        case EPermission::Update:
            return "nbsInternal.disks.update";
        case EPermission::Delete:
            return "nbsInternal.disks.delete";
        case EPermission::Read:
            return "nbsInternal.disks.read";
        case EPermission::Write:
            return "nbsInternal.disks.write";
        case EPermission::MAX:
            Y_ABORT("EPermission::MAX is invalid");
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPermissionList CreatePermissionList(
    std::initializer_list<EPermission> permissions)
{
    TPermissionList result;
    for (EPermission permission : permissions) {
        result.Set(static_cast<size_t>(permission));
    }
    return result;
}

TVector<TString> GetPermissionStrings(const TPermissionList& permissions)
{
    TVector<TString> result;
    Y_FOR_EACH_BIT(permission, permissions) {
        result.push_back(ToString(static_cast<EPermission>(permission)));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::CmsAction:
            return CreatePermissionList({EPermission::Update});
        // The following 5 don't deal with user data.
        case EBlockStoreRequest::Ping:
        case EBlockStoreRequest::UploadClientMetrics:
        case EBlockStoreRequest::DiscoverInstances:
        case EBlockStoreRequest::QueryAvailableStorage:
        case EBlockStoreRequest::QueryAgentsInfo:
            return CreatePermissionList({});
        // UnmountVolume can't expose or corrupt any user data.
        case EBlockStoreRequest::UnmountVolume:
            return CreatePermissionList({EPermission::Update});
        // The following 5 can only be sent via an active endpoint.
        case EBlockStoreRequest::ReadBlocks:
        case EBlockStoreRequest::WriteBlocks:
        case EBlockStoreRequest::ZeroBlocks:
        case EBlockStoreRequest::ReadBlocksLocal:
        case EBlockStoreRequest::WriteBlocksLocal:
            return CreatePermissionList({});

        case EBlockStoreRequest::MountVolume:
            Y_ABORT("MountVolume must have been handled separately");

        case EBlockStoreRequest::AssignVolume:
            return CreatePermissionList({EPermission::Update});
        case EBlockStoreRequest::CreateVolume:
        case EBlockStoreRequest::CreateVolumeFromDevice:
        case EBlockStoreRequest::CreatePlacementGroup:
            return CreatePermissionList({EPermission::Create});
        case EBlockStoreRequest::DestroyVolume:
        case EBlockStoreRequest::DestroyPlacementGroup:
            return CreatePermissionList({EPermission::Delete});
        case EBlockStoreRequest::ResizeVolume:
        case EBlockStoreRequest::AlterVolume:
        case EBlockStoreRequest::AlterPlacementGroupMembership:
        case EBlockStoreRequest::ResumeDevice:
            return CreatePermissionList({EPermission::Update});
        case EBlockStoreRequest::StatVolume:
            return CreatePermissionList({EPermission::Get});
        case EBlockStoreRequest::CreateCheckpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        case EBlockStoreRequest::DeleteCheckpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        case EBlockStoreRequest::GetChangedBlocks:
        case EBlockStoreRequest::GetCheckpointStatus:
            return CreatePermissionList({EPermission::Read});
        case EBlockStoreRequest::DescribeVolume:
        case EBlockStoreRequest::DescribeVolumeModel:
        case EBlockStoreRequest::DescribePlacementGroup:
        case EBlockStoreRequest::DescribeEndpoint:
            return CreatePermissionList({EPermission::Get});
        case EBlockStoreRequest::ListVolumes:
        case EBlockStoreRequest::ListPlacementGroups:
            return CreatePermissionList({EPermission::List});

        case EBlockStoreRequest::StartEndpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        case EBlockStoreRequest::StopEndpoint:
        case EBlockStoreRequest::KickEndpoint:
        case EBlockStoreRequest::RefreshEndpoint:
            return CreatePermissionList({EPermission::Update});

        case EBlockStoreRequest::ListEndpoints:
        case EBlockStoreRequest::ListKeyrings:
            return CreatePermissionList({EPermission::List});

        case EBlockStoreRequest::ExecuteAction:
            Y_ABORT("ExecuteAction must have been handled separately");
        case EBlockStoreRequest::DescribeDiskRegistryConfig:
        case EBlockStoreRequest::UpdateDiskRegistryConfig:
        case EBlockStoreRequest::UpdateThrottlingConfig:
            return TPermissionList().Flip();  // Require admin permissions.

        case EBlockStoreRequest::CreateVolumeLink:
        case EBlockStoreRequest::DestroyVolumeLink:
                return CreatePermissionList({EPermission::Update});

        case EBlockStoreRequest::RemoveVolumeClient:
            return CreatePermissionList({EPermission::Delete});

        case EBlockStoreRequest::MAX:
            Y_ABORT("EBlockStoreRequest::MAX is not valid");
    }
}

TPermissionList GetMountPermissions(
    const NProto::TMountVolumeRequest& request)
{
    const auto mode = request.GetVolumeAccessMode();
    switch (mode) {
        case NProto::VOLUME_ACCESS_READ_ONLY:
        case NProto::VOLUME_ACCESS_USER_READ_ONLY:
            return CreatePermissionList({EPermission::Read});
        case NProto::VOLUME_ACCESS_READ_WRITE:
        case NProto::VOLUME_ACCESS_REPAIR:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        default:
            // In case we get unknown volume access mode, assume
            // read-only permission.
            Y_DEBUG_ABORT_UNLESS(false, "Unknown EVolumeAccessMode: %d", mode);
            return CreatePermissionList({EPermission::Read});
    }
}

TPermissionList GetRequestPermissions(
    const NProto::TMountVolumeRequest& request)
{
    return GetMountPermissions(request);
}

TPermissionList GetRequestPermissions(
    const NProto::TExecuteActionRequest& request)
{
    TString action = request.GetAction();
    action.to_lower();

    auto perms = [] (TString name, std::initializer_list<EPermission> lst) {
        return std::pair {name, NCloud::CreatePermissionList(lst)};
    };

    static const THashMap<TString, TPermissionList> actions = {
        // Get
        perms("backupdiskregistrystate", {EPermission::Get}),
        perms("checkblob", {EPermission::Get}),
        perms("describevolume", {EPermission::Get}),
        perms("getcompactionstatus", {EPermission::Get}),
        perms("getpartitioninfo", {EPermission::Get}),
        perms("getrebuildmetadatastatus", {EPermission::Get}),
        perms("getscandiskstatus", {EPermission::Get}),
        perms("scandisk", {EPermission::Get}),
        perms("getdiskregistrytabletinfo", {EPermission::Get}),

        // Update
        perms("configurevolumebalancer", {EPermission::Update}),
        perms("drainnode", {EPermission::Update}),
        perms("killtablet", {EPermission::Update}),
        perms("reallocatedisk", {EPermission::Update}),
        perms("rebindvolumes", {EPermission::Update}),
        perms("setuserid", {EPermission::Update}),
        perms("cms", {EPermission::Update}),

        // Delete
        perms("deletecheckpointdata", {EPermission::Delete}),

        // Read
        perms("describeblocks", {EPermission::Read}),
    };

    auto it = actions.find(action);
    if (it != actions.end()) {
        return it->second;
    }

    return TPermissionList().Flip();
}

}  // namespace NCloud::NBlockStore
