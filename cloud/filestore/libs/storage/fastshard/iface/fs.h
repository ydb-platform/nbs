#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemShard
{
    virtual ~IFileSystemShard() = default;

    virtual NThreading::TFuture<NProtoPrivate::TGetNodeAttrBatchResponse>
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request) = 0;
    virtual NThreading::TFuture<NProto::TGetNodeAttrResponse>
    GetNodeAttr(NProto::TGetNodeAttrRequest request) = 0;
    virtual NThreading::TFuture<NProto::TSetNodeAttrResponse>
    SetNodeAttr(NProto::TSetNodeAttrRequest request) = 0;

    virtual NThreading::TFuture<NProto::TAccessNodeResponse>
    AccessNode(NProto::TAccessNodeRequest request) = 0;

    virtual NThreading::TFuture<NProto::TCreateNodeResponse>
    CreateNode(NProto::TCreateNodeRequest request) = 0;
    virtual NThreading::TFuture<NProto::TUnlinkNodeResponse>
    UnlinkNode(NProto::TUnlinkNodeRequest request) = 0;

    virtual NThreading::TFuture<NProto::TCreateHandleResponse>
    CreateHandle(NProto::TCreateHandleRequest request) = 0;
    virtual NThreading::TFuture<NProto::TDestroyHandleResponse>
    DestroyHandle(NProto::TDestroyHandleRequest request) = 0;

    virtual NThreading::TFuture<NProto::TAllocateDataResponse>
    AllocateData(NProto::TAllocateDataRequest request) = 0;

    virtual NThreading::TFuture<NProto::TAcquireLockResponse>
    AcquireLock(NProto::TAcquireLockRequest request) = 0;
    virtual NThreading::TFuture<NProto::TReleaseLockResponse>
    ReleaseLock(NProto::TReleaseLockRequest request) = 0;
    virtual NThreading::TFuture<NProto::TTestLockResponse>
    TestLock(NProto::TTestLockRequest request) = 0;

    virtual NThreading::TFuture<NProto::TWriteDataResponse>
    WriteData(NProto::TWriteDataRequest request) = 0;
    virtual NThreading::TFuture<NProto::TReadDataResponse>
    ReadData(NProto::TReadDataRequest request) = 0;

    virtual NThreading::TFuture<NProto::TRemoveNodeXAttrResponse>
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request) = 0;
    virtual NThreading::TFuture<NProto::TGetNodeXAttrResponse>
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request) = 0;
    virtual NThreading::TFuture<NProto::TSetNodeXAttrResponse>
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request) = 0;
    virtual NThreading::TFuture<NProto::TListNodeXAttrResponse>
    ListNodeXAttr(NProto::TListNodeXAttrRequest request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateFileSystemShardStub();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
