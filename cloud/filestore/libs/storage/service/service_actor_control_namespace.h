#pragma once

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
// Reserved inos use the shard number one past MaxShardCount, which real
// shards never get assigned, so they can't collide with real nodes.
static_assert(
    MaxShardCount < Max<ui16>(),
    "the control namespace reserves the highest representable shard "
    "number - MaxShardCount must leave it unassigned to real shards");

constexpr ui32 ControlNamespaceShardNo = MaxShardCount + 1;

constexpr ui64 ControlDirIno = ShardedId(1, ControlNamespaceShardNo);
constexpr ui64 ControlFsIdFileIno = ShardedId(2, ControlNamespaceShardNo);

static_assert(ExtractShardNo(ControlDirIno) == ControlNamespaceShardNo);
static_assert(ExtractShardNo(ControlFsIdFileIno) == ControlNamespaceShardNo);
static_assert(ControlNamespaceShardNo > MaxShardCount);

constexpr TStringBuf ControlFsIdFileName = "fsid";

enum class EControlNamespaceEntry
{
    None,
    ControlDir,
    FsId,
    // Under the control dir, but not a name it actually exposes.
    Unknown,
};

// Self-lookup form: classifies an already-resolved ino.
EControlNamespaceEntry ClassifyControlNamespaceEntry(ui64 nodeId);

// By-name form: classifies a (parent, name) pair, e.g. from a lookup or
// a create/rename target.
EControlNamespaceEntry ClassifyControlNamespaceEntry(
    ui64 parentId,
    TStringBuf name,
    TStringBuf dirName);

void FillControlDirAttr(NProto::TNodeAttr& attr);
void FillControlFsIdAttr(NProto::TNodeAttr& attr, const TString& fileSystemId);

NProto::TError ControlNamespaceReadOnlyError();

////////////////////////////////////////////////////////////////////////////////
// Synthesizes the response for a control-namespace entity reached via
// ForwardRequestToShard's generic dispatch (self-lookup-by-ino forms of
// GetNodeAttr/CreateHandle, and ConfirmCreateHandle/DestroyHandle, which have
// no dedicated TryHandleControlNamespaceXxx of their own).
//
// Default: reject. Correct as-is for every method here that isn't one of
// open/close/read/write/stat/ls - notably AccessNode: a bare access(2)/
// faccessat(2) check gets turned away same as any other non-essential op,
// it doesn't gate the real open/read/stat paths above. Methods with real
// content get a specialization below.
template <typename TMethod>
std::unique_ptr<typename TMethod::TResponse> BuildControlNamespaceResponse(
    const typename TMethod::TRequest::TPtr& ev,
    const TString& fileSystemId)
{
    Y_UNUSED(ev);
    Y_UNUSED(fileSystemId);
    return std::make_unique<typename TMethod::TResponse>(
        ControlNamespaceReadOnlyError());
}

template <>
std::unique_ptr<TEvService::TGetNodeAttrMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TGetNodeAttrMethod>(
    const TEvService::TGetNodeAttrMethod::TRequest::TPtr& ev,
    const TString& fileSystemId);

template <>
std::unique_ptr<TEvService::TCreateHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TCreateHandleMethod>(
    const TEvService::TCreateHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId);

template <>
std::unique_ptr<TEvService::TConfirmCreateHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TConfirmCreateHandleMethod>(
    const TEvService::TConfirmCreateHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId);

template <>
std::unique_ptr<TEvService::TDestroyHandleMethod::TResponse>
BuildControlNamespaceResponse<TEvService::TDestroyHandleMethod>(
    const TEvService::TDestroyHandleMethod::TRequest::TPtr& ev,
    const TString& fileSystemId);

}   // namespace NCloud::NFileStore::NStorage
