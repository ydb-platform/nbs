#include "profile_log_events.h"

#include "profile_log.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/public/api/protos/checkpoint.pb.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestProfileLog
    : IProfileLog
{
    IProfileLog::TRecord Record;

    void Start() override
    {}

    void Stop() override
    {}

    void Write(IProfileLog::TRecord record) override
    {
        Record = std::move(record);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProfileLogEventsTest)
{
    Y_UNIT_TEST(ShouldCreateHandleRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "handle";
        const auto flags = 3;
        const auto mode = 7;

        NProto::TCreateHandleRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);
        req.SetFlags(flags);
        req.SetMode(mode);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT_VALUES_EQUAL(flags, nodeInfo.GetFlags());
        UNIT_ASSERT_VALUES_EQUAL(mode, nodeInfo.GetMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldDestroyHandleRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;

        NProto::TDestroyHandleRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, nodeInfo.GetHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldReadDataRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto offset = 56;
        const auto length = 78;

        NProto::TReadDataRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOffset(offset);
        req.SetLength(length);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& rangeInfo = profileLogRequest.GetRanges().at(0);
        UNIT_ASSERT_VALUES_EQUAL(nodeId, rangeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, rangeInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(offset, rangeInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, rangeInfo.GetBytes());
    }

    Y_UNIT_TEST(ShouldWriteDataRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto offset = 56;
        const TString buffer = "buffer";

        NProto::TWriteDataRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOffset(offset);
        req.SetBuffer(buffer);
        req.SetDataSize(buffer.size());

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& rangeInfo = profileLogRequest.GetRanges().at(0);
        UNIT_ASSERT_VALUES_EQUAL(nodeId, rangeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, rangeInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(offset, rangeInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(buffer.length(), rangeInfo.GetBytes());
    }

    Y_UNIT_TEST(ShouldAllocateDataRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto offset = 56;
        const auto length = 78;

        NProto::TAllocateDataRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOffset(offset);
        req.SetLength(length);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& rangeInfo = profileLogRequest.GetRanges().at(0);
        UNIT_ASSERT_VALUES_EQUAL(nodeId, rangeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, rangeInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(offset, rangeInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, rangeInfo.GetBytes());
    }

    Y_UNIT_TEST(ShouldTruncateDataRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto length = 78;

        NProto::TTruncateDataRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetLength(length);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& rangeInfo = profileLogRequest.GetRanges().at(0);
        UNIT_ASSERT_VALUES_EQUAL(nodeId, rangeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, rangeInfo.GetHandle());
        UNIT_ASSERT(!rangeInfo.HasOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, rangeInfo.GetBytes());
    }

    Y_UNIT_TEST(ShouldAcquireLockRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto owner = 56;
        const auto offset = 78;
        const auto length = 90;
        const auto lockType = NProto::ELockType::E_EXCLUSIVE;
        const auto pid = 123;

        NProto::TAcquireLockRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOwner(owner);
        req.SetOffset(offset);
        req.SetLength(length);
        req.SetLockType(lockType);
        req.SetPid(pid);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(profileLogRequest.HasLockInfo());

        const auto& lockInfo = profileLogRequest.GetLockInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, lockInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, lockInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(owner, lockInfo.GetOwner());
        UNIT_ASSERT_VALUES_EQUAL(offset, lockInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, lockInfo.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(lockType), lockInfo.GetType());
        UNIT_ASSERT(!lockInfo.HasConflictedOwner());
        UNIT_ASSERT(!lockInfo.HasConflictedOffset());
        UNIT_ASSERT(!lockInfo.HasConflictedLength());
    }

    Y_UNIT_TEST(ShouldReleaseLockRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto owner = 56;
        const auto offset = 78;
        const auto length = 90;

        NProto::TReleaseLockRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOwner(owner);
        req.SetOffset(offset);
        req.SetLength(length);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(profileLogRequest.HasLockInfo());

        const auto& lockInfo = profileLogRequest.GetLockInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, lockInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, lockInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(owner, lockInfo.GetOwner());
        UNIT_ASSERT_VALUES_EQUAL(offset, lockInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, lockInfo.GetLength());
        UNIT_ASSERT(!lockInfo.HasType());
        UNIT_ASSERT(!lockInfo.HasConflictedOwner());
        UNIT_ASSERT(!lockInfo.HasConflictedOffset());
        UNIT_ASSERT(!lockInfo.HasConflictedLength());
    }

    Y_UNIT_TEST(ShouldTestLockRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto owner = 56;
        const auto offset = 78;
        const auto length = 90;
        const auto lockType = NProto::ELockType::E_EXCLUSIVE;
        const auto pid = 123;

        NProto::TTestLockRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.SetOwner(owner);
        req.SetOffset(offset);
        req.SetLength(length);
        req.SetLockType(lockType);
        req.SetPid(pid);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(profileLogRequest.HasLockInfo());

        const auto& lockInfo = profileLogRequest.GetLockInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, lockInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, lockInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(owner, lockInfo.GetOwner());
        UNIT_ASSERT_VALUES_EQUAL(offset, lockInfo.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, lockInfo.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(lockType), lockInfo.GetType());
        UNIT_ASSERT(!lockInfo.HasConflictedOwner());
        UNIT_ASSERT(!lockInfo.HasConflictedOffset());
        UNIT_ASSERT(!lockInfo.HasConflictedLength());
    }

    Y_UNIT_TEST(ShouldCreateNodeRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "node";

        NProto::TCreateNodeRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNewParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldUnlinkNodeRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "node";

        NProto::TUnlinkNodeRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldRenameNodeRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "node";
        const auto newNodeId = 34;
        const auto newName = "new_node";

        NProto::TRenameNodeRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);
        req.SetNewParentId(newNodeId);
        req.SetNewName(newName);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT_VALUES_EQUAL(newNodeId, nodeInfo.GetNewParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(newName, nodeInfo.GetNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldAccessNodeRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto mask = 34;

        NProto::TAccessNodeRequest req;
        req.SetNodeId(nodeId);
        req.SetMask(mask);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT_VALUES_EQUAL(mask, nodeInfo.GetFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldListNodesRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;

        NProto::TListNodesRequest req;
        req.SetNodeId(nodeId);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldReadLinkRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;

        NProto::TReadLinkRequest req;
        req.SetNodeId(nodeId);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldSetNodeAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto mode = 56;
        const auto flags = 78;

        NProto::TSetNodeAttrRequest req;
        req.SetNodeId(nodeId);
        req.SetHandle(handle);
        req.MutableUpdate()->SetMode(mode);
        req.SetFlags(flags);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT_VALUES_EQUAL(flags, nodeInfo.GetFlags());
        UNIT_ASSERT_VALUES_EQUAL(mode, nodeInfo.GetMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, nodeInfo.GetHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldGetNodeAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "name";
        const auto handle = 34;
        const auto flags = 78;

        NProto::TGetNodeAttrRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);
        req.SetHandle(handle);
        req.SetFlags(flags);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT_VALUES_EQUAL(flags, nodeInfo.GetFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, nodeInfo.GetHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldSetNodeXAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "name";
        const auto value = "value";
        const auto flags = 34;

        NProto::TSetNodeXAttrRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);
        req.SetValue(value);
        req.SetFlags(flags);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(value, nodeInfo.GetNewNodeName());
        UNIT_ASSERT_VALUES_EQUAL(flags, nodeInfo.GetFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldGetNodeXAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "name";

        NProto::TGetNodeXAttrRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldListNodeXAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;

        NProto::TListNodeXAttrRequest req;
        req.SetNodeId(nodeId);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldRemoveNodeXAttrRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto name = "name";

        NProto::TRemoveNodeXAttrRequest req;
        req.SetNodeId(nodeId);
        req.SetName(name);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(name, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldCreateCheckpointRequestInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto checkpointId = "checkpoint";

        NProto::TCreateCheckpointRequest req;
        req.SetNodeId(nodeId);
        req.SetCheckpointId(checkpointId);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(checkpointId, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldDestroyCheckpointRequestInitializeFieldsCorrectly)
    {
        const auto checkpointId = "checkpoint";

        NProto::TDestroyCheckpointRequest req;
        req.SetCheckpointId(checkpointId);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, req);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(checkpointId, nodeInfo.GetNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT(!nodeInfo.HasSize());
    }

    Y_UNIT_TEST(ShouldCreateHandleResponseInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto handle = 34;
        const auto size = 56;

        NProto::TCreateHandleResponse res;
        res.MutableNodeAttr()->SetId(nodeId);
        res.MutableNodeAttr()->SetSize(size);
        res.SetHandle(handle);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(handle, nodeInfo.GetHandle());
        UNIT_ASSERT_VALUES_EQUAL(size, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldTestLockResponseInitializeFieldsCorrectly)
    {
        const auto owner = 12;
        const auto offset = 34;
        const auto length = 56;
        const auto lockType = NProto::ELockType::E_EXCLUSIVE;
        const auto pid = 123;

        NProto::TTestLockResponse res;
        res.SetOwner(owner);
        res.SetOffset(offset);
        res.SetLength(length);
        res.SetLockType(lockType);
        res.SetPid(pid);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(profileLogRequest.HasLockInfo());

        const auto& lockInfo = profileLogRequest.GetLockInfo();
        UNIT_ASSERT(!lockInfo.HasNodeId());
        UNIT_ASSERT(!lockInfo.HasHandle());
        UNIT_ASSERT(!lockInfo.HasOwner());
        UNIT_ASSERT(!lockInfo.HasOffset());
        UNIT_ASSERT(!lockInfo.HasLength());
        UNIT_ASSERT(!lockInfo.HasType());
        UNIT_ASSERT_VALUES_EQUAL(owner, lockInfo.GetConflictedOwner());
        UNIT_ASSERT_VALUES_EQUAL(offset, lockInfo.GetConflictedOffset());
        UNIT_ASSERT_VALUES_EQUAL(length, lockInfo.GetConflictedLength());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(lockType),
            lockInfo.GetConflictedLockType());
        UNIT_ASSERT_VALUES_EQUAL(pid, lockInfo.GetConflictedPid());
    }

    Y_UNIT_TEST(ShouldCreateNodeResponseInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto mode = 3;
        const auto size = 45;

        NProto::TCreateNodeResponse res;
        res.MutableNode()->SetId(nodeId);
        res.MutableNode()->SetMode(mode);
        res.MutableNode()->SetSize(size);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT_VALUES_EQUAL(mode, nodeInfo.GetMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(size, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldListNodesResponseInitializeFieldsCorrectly)
    {
        const std::array<TString, 3> names = {"name_1", "name_2", "name_3"};

        NProto::TListNodesResponse res;
        for (const auto& name : names) {
            res.AddNames(name);
        }

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(names.size(), nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldSetNodeAttrResponseInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto size = 34;

        NProto::TSetNodeAttrResponse res;
        res.MutableNode()->SetId(nodeId);
        res.MutableNode()->SetSize(size);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(size, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldGetNodeAttrResponseInitializeFieldsCorrectly)
    {
        const auto nodeId = 12;
        const auto mode = 34;
        const auto size = 56;

        NProto::TGetNodeAttrResponse res;
        res.MutableNode()->SetId(nodeId);
        res.MutableNode()->SetMode(mode);
        res.MutableNode()->SetSize(size);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT_VALUES_EQUAL(mode, nodeInfo.GetMode());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, nodeInfo.GetNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(size, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldSetNodeXAttrResponseInitializeFieldsCorrectly)
    {
        const auto version = 12;

        NProto::TSetNodeXAttrResponse res;
        res.SetVersion(version);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(version, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldGetNodeXAttrResponseInitializeFieldsCorrectly)
    {
        const TString value = "value";
        const auto version = 12;

        NProto::TGetNodeXAttrResponse res;
        res.SetValue(value);
        res.SetVersion(version);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT_VALUES_EQUAL(value, nodeInfo.GetNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(version, nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldListNodeXAttrResponseInitializeFieldsCorrectly)
    {
        const std::array<TString, 2> names = {"name_1", "name_2"};

        NProto::TListNodeXAttrResponse res;
        for (const auto& name : names) {
            res.AddNames(name);
        }

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);

        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.RangesSize());
        UNIT_ASSERT(profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        const auto& nodeInfo = profileLogRequest.GetNodeInfo();
        UNIT_ASSERT(!nodeInfo.HasParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNodeName());
        UNIT_ASSERT(!nodeInfo.HasNewParentNodeId());
        UNIT_ASSERT(!nodeInfo.HasNewNodeName());
        UNIT_ASSERT(!nodeInfo.HasFlags());
        UNIT_ASSERT(!nodeInfo.HasMode());
        UNIT_ASSERT(!nodeInfo.HasNodeId());
        UNIT_ASSERT(!nodeInfo.HasHandle());
        UNIT_ASSERT_VALUES_EQUAL(names.size(), nodeInfo.GetSize());
    }

    Y_UNIT_TEST(ShouldReadDataResponseInitializeFieldsCorrectly)
    {
        NProto::TReadDataResponse res;
        constexpr auto Size = 42;
        TString data{Size, ' '};
        res.SetBuffer(data);

        NProto::TProfileLogRequestInfo profileLogRequest;
        FinalizeProfileLogRequestInfo(profileLogRequest, res);
        UNIT_ASSERT_VALUES_EQUAL(
            Size,
            profileLogRequest.GetRanges(0).GetActualBytes());
    }

    Y_UNIT_TEST(ShouldGetCorrectFuseRequestName)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "Unknown",
            GetFileStoreFuseRequestName(NFuse::EFileStoreFuseRequest::MIN));

        UNIT_ASSERT_VALUES_EQUAL(
            "Flush",
            GetFileStoreFuseRequestName(NFuse::EFileStoreFuseRequest::Flush));

        UNIT_ASSERT_VALUES_EQUAL(
            "Fsync",
            GetFileStoreFuseRequestName(NFuse::EFileStoreFuseRequest::Fsync));

        UNIT_ASSERT_VALUES_EQUAL(
            "Unknown",
            GetFileStoreFuseRequestName(NFuse::EFileStoreFuseRequest::MAX));
    }

    Y_UNIT_TEST(ShouldCorrectlyInitProfileLogRequest)
    {
        const auto timestamp = TInstant::MilliSeconds(12);
        const auto requestType = NFuse::EFileStoreFuseRequest::Flush;

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, requestType, timestamp);

        UNIT_ASSERT_VALUES_EQUAL(
            timestamp.MicroSeconds(),
            profileLogRequest.GetTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(requestType),
            profileLogRequest.GetRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.GetRanges().size());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldCorrectlyFinalizeProfileLogRequest)
    {
        const auto startTs = TInstant::MilliSeconds(2);
        const auto endTs = TInstant::MilliSeconds(12);
        const TString fileSystemId = "test_filesystem";

        NCloud::NProto::TError error;
        error.SetCode(1);

        auto profileLog = std::make_shared<TTestProfileLog>();

        NProto::TProfileLogRequestInfo profileLogRequest;
        profileLogRequest.SetTimestampMcs(startTs.MicroSeconds());
        NFuse::FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            endTs,
            fileSystemId,
            error,
            profileLog);

        UNIT_ASSERT_VALUES_EQUAL(fileSystemId, profileLog->Record.FileSystemId);
        UNIT_ASSERT_VALUES_EQUAL(
            startTs.MicroSeconds(),
            profileLog->Record.Request.GetTimestampMcs());
        UNIT_ASSERT_VALUES_EQUAL(
            (endTs - startTs).MicroSeconds(),
            profileLog->Record.Request.GetDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT_VALUES_EQUAL(
            error.GetCode(),
            profileLog->Record.Request.GetErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Record.Request.GetRanges().size());
        UNIT_ASSERT(!profileLog->Record.Request.HasNodeInfo());
        UNIT_ASSERT(!profileLog->Record.Request.HasLockInfo());
    }
}

}   // namespace NCloud::NFileStore
