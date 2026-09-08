#pragma once

#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/generic/maybe.h>
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

// What a node or (parent, name) pair resolves to in the control namespace
enum class EControlNamespaceEntry
{
    ControlDir, // the root control dir itself
    FsId, // "fsid" file under the control dir, exposes the filesystem ID
    Unknown, // under the control dir but not a known name
};

TMaybe<EControlNamespaceEntry> ClassifyControlNamespaceEntry(ui64 nodeId);

TMaybe<EControlNamespaceEntry> ClassifyControlNamespaceEntry(
    ui64 parentId,
    TStringBuf name,
    TStringBuf controlNamespaceDirName);

void FillControlDirAttr(NProto::TNodeAttr& attr);
void FillControlFsIdAttr(NProto::TNodeAttr& attr, const TString& fileSystemId);

NProto::TError ControlNamespaceNotPermittedError();

}   // namespace NCloud::NFileStore::NStorage
