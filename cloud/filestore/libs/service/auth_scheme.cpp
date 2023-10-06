#include "auth_scheme.h"

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
        case EFileStoreRequest::SubscribeSession:
        case EFileStoreRequest::GetSessionEvents:
        case EFileStoreRequest::GetSessionEventsStream:
        case EFileStoreRequest::ResolvePath:
        case EFileStoreRequest::CreateNode:
        case EFileStoreRequest::UnlinkNode:
        case EFileStoreRequest::RenameNode:
        case EFileStoreRequest::AccessNode:
        case EFileStoreRequest::ReadLink:
        case EFileStoreRequest::ListNodes:
        case EFileStoreRequest::SetNodeAttr:
        case EFileStoreRequest::GetNodeAttr:
        case EFileStoreRequest::SetNodeXAttr:
        case EFileStoreRequest::GetNodeXAttr:
        case EFileStoreRequest::ListNodeXAttr:
        case EFileStoreRequest::RemoveNodeXAttr:
        case EFileStoreRequest::CreateHandle:
        case EFileStoreRequest::DestroyHandle:
        case EFileStoreRequest::ReadData:
        case EFileStoreRequest::WriteData:
        case EFileStoreRequest::AllocateData:
        case EFileStoreRequest::AcquireLock:
        case EFileStoreRequest::ReleaseLock:
        case EFileStoreRequest::TestLock:
            return CreatePermissionList({});

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

        case EFileStoreRequest::ListEndpoints:
            return CreatePermissionList({EPermission::List});

        case EFileStoreRequest::ExecuteAction:
            return TPermissionList().Flip();  // Require admin permissions.

        case EFileStoreRequest::MAX:
            Y_FAIL("EFileStoreRequest::MAX is not valid");
    }
}

}  // namespace NCloud::NBlockStore
