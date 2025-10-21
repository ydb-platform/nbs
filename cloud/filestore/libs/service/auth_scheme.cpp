#include "auth_scheme.h"

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EFileStoreRequest requestType)
{
    switch (requestType) {
        case EFileStoreRequest::Ping:
            return CreatePermissionList({});
        case EFileStoreRequest::CreateSession:
        case EFileStoreRequest::DestroySession:
        case EFileStoreRequest::ResetSession:
        case EFileStoreRequest::PingSession:
        case EFileStoreRequest::ToggleServiceState:
        case EFileStoreRequest::SubscribeSession:
        case EFileStoreRequest::GetSessionEvents:
        case EFileStoreRequest::GetSessionEventsStream:
            return CreatePermissionList({EPermission::Update});
        case EFileStoreRequest::ResolvePath:
        case EFileStoreRequest::ListNodes:
        case EFileStoreRequest::GetNodeAttr:
        case EFileStoreRequest::GetNodeXAttr:
        case EFileStoreRequest::ListNodeXAttr:
        case EFileStoreRequest::ReadData:
        case EFileStoreRequest::TestLock:
        case EFileStoreRequest::DescribeData:
        case EFileStoreRequest::ReadBlob:
            return CreatePermissionList({EPermission::Read});
        case EFileStoreRequest::CreateNode:
        case EFileStoreRequest::UnlinkNode:
        case EFileStoreRequest::RenameNode:
        case EFileStoreRequest::AccessNode:
        case EFileStoreRequest::ReadLink:
        case EFileStoreRequest::SetNodeAttr:
        case EFileStoreRequest::SetNodeXAttr:
        case EFileStoreRequest::RemoveNodeXAttr:
        case EFileStoreRequest::CreateHandle:
        case EFileStoreRequest::DestroyHandle:
        case EFileStoreRequest::WriteData:
        case EFileStoreRequest::AllocateData:
        case EFileStoreRequest::AcquireLock:
        case EFileStoreRequest::ReleaseLock:
        case EFileStoreRequest::GenerateBlobIds:
        case EFileStoreRequest::WriteBlob:
        case EFileStoreRequest::AddData:
        case EFileStoreRequest::Fsync:
        case EFileStoreRequest::FsyncDir:
            return CreatePermissionList({EPermission::Write});

        case EFileStoreRequest::AddClusterNode:
        case EFileStoreRequest::AddClusterClients:
        case EFileStoreRequest::UpdateCluster:
            return CreatePermissionList({EPermission::Update});
        case EFileStoreRequest::CreateFileStore:
            return CreatePermissionList({EPermission::Create});
        case EFileStoreRequest::DestroyFileStore:
        case EFileStoreRequest::RemoveClusterNode:
        case EFileStoreRequest::RemoveClusterClients:
            return CreatePermissionList({EPermission::Delete});
        case EFileStoreRequest::ResizeFileStore:
        case EFileStoreRequest::AlterFileStore:
            return CreatePermissionList({EPermission::Update});
        case EFileStoreRequest::GetFileStoreInfo:
        case EFileStoreRequest::StatFileStore:
            return CreatePermissionList({EPermission::Get});
        case EFileStoreRequest::CreateCheckpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        case EFileStoreRequest::DestroyCheckpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});
        case EFileStoreRequest::DescribeFileStoreModel:
            return CreatePermissionList({EPermission::Get});
        case EFileStoreRequest::ListFileStores:
        case EFileStoreRequest::ListClusterNodes:
        case EFileStoreRequest::ListClusterClients:
            return CreatePermissionList({EPermission::List});

        case EFileStoreRequest::StartEndpoint:
        case EFileStoreRequest::StopEndpoint:
        case EFileStoreRequest::KickEndpoint:
            return CreatePermissionList({
                EPermission::Read,
                EPermission::Write});

        case EFileStoreRequest::ExecuteAction:
            Y_ABORT("ExecuteAction must have been handled separately");

        case EFileStoreRequest::ListEndpoints:
            return CreatePermissionList({EPermission::List});

        case EFileStoreRequest::MAX:
            Y_ABORT("EFileStoreRequest::MAX is not valid");
    }
}

TPermissionList GetRequestPermissions(
    const NProto::TExecuteActionRequest& request,
    const TVector<TString>& actionsNoAuth)
{
    TString action = request.GetAction();
    action.to_lower();

    auto perms = [] (TString name, TPermissionList lst) {
        return std::pair {name, std::move(lst)};
    };

    if (!!FindPtr(actionsNoAuth, action)) {
        return TPermissionList();
    }

    static const THashMap<TString, TPermissionList> actions = {
        // Get
        perms("getstorageconfigfields", CreatePermissionList({EPermission::Get})),
        perms("listlocalfilestores", CreatePermissionList({EPermission::Get})),

        // Update
        perms("draintablets", CreatePermissionList({EPermission::Update})),

        // Admin
        perms("changestorageconfig", TPermissionList().Flip()),
        perms("restarttablet", TPermissionList().Flip())
    };

    auto it = actions.find(action);
    if (it != actions.end()) {
        return it->second;
    }

    return TPermissionList().Flip();
}

}  // namespace NCloud::NBlockStore
