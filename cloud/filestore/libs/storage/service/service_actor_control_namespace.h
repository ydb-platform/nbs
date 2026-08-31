#pragma once

#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/filestore/public/api/protos/fs.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
// Control namespace: a hidden ".filestore-ctl" directory under the root of
// every kikimr-backed filesystem, synthesized here in the storage-service
// actor - the single actor every kikimr request is routed through
// (TRequestActor -> MakeStorageServiceId(), see
// service_kikimr/request_actor.h) before ever reaching a tablet. Gated
// behind TStorageConfig::EnableControlNamespace (default off).
//
// Reserved inos are ShardedId(id, Max<ui16>()) - the highest
// representable shard number, one past NStorage::MaxShardCount (see
// core/model.h) - so they can never collide with a real shard's nodes.
static_assert(
    MaxShardCount < Max<ui16>(),
    "the control namespace reserves the highest representable shard "
    "number - MaxShardCount must leave it unassigned to real shards");

const ui64 ControlDirIno = ShardedId(1, Max<ui16>());
const ui64 ControlFsIdFileIno = ShardedId(2, Max<ui16>());

constexpr TStringBuf ControlDirName = ".filestore-ctl";
constexpr TStringBuf ControlFsIdFileName = "fsid";

// True for the control dir itself or any of its children.
bool IsControlNamespaceNode(ui64 ino);

void FillControlDirAttr(NProto::TNodeAttr& attr);
void FillControlFsIdAttr(NProto::TNodeAttr& attr, const TString& fileSystemId);

NProto::TError ControlNamespaceReadOnlyError();

}   // namespace NCloud::NFileStore::NStorage
