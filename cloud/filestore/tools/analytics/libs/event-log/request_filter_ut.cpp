#include "request_filter.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
    : public NUnitTest::TBaseFixture
{
    NProto::TProfileLogRecord FirstRecord;
    NProto::TProfileLogRecord SecondRecord;
    NProto::TProfileLogRecord ThirdRecord;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        SetupFirstRecord();
        SetupSecondRecord();
        SetupThirdRecord();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

private:
    void SetupFirstRecord()
    {
        FirstRecord.SetFileSystemId("fs1");

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                0,                                                // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                1,                           // newParentNodeId
                "test_node",                 // newNodeName
                {},                          // flags
                7,                           // mode
                2,                           // nodeId
                {},                          // handle
                64);                         // size
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                10,                                               // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::AccessNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                7,                           // flags
                {},                          // mode
                2,                           // nodeId
                {},                          // handle
                {});                         // size
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                           // request
                20,                                                 // timestmpMcs
                10,                                                 // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateHandle), // requestType
                0);                                                 // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                1,                           // parentNodeId
                "test_node",                 // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                3,                           // flags
                5,                           // mode
                2,                           // nodeId
                100,                         // handle
                64);                         // size
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                          // request
                30,                                                // timestmpMcs
                10,                                                // durationMcs
                static_cast<ui32>(EFileStoreRequest::AcquireLock), // requestType
                0);                                                // errorCode
            SetupLockInfo(
                *request->MutableLockInfo(), // lockInfo
                2,                           // nodeId
                101,                         // handle
                5,                           // owner
                0,                           // offset
                32,                          // length
                0,                           // lockType
                {},                          // conflictedOwner
                {},                          // conflictedOffset
                {});                         // conflictedLength
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                40,                                               // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::RenameNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                1,                           // parentNodeId
                "test_node",                 // nodeName
                1,                           // newParentNodeId
                "test_node_2",               // newNodeName
                {},                          // flags
                {},                          // mode
                {},                          // nodeId
                {},                          // handle
                {});                         // size
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                          // request
                50,                                                // timestmpMcs
                10,                                                // durationMcs
                static_cast<ui32>(EFileStoreRequest::ReleaseLock), // requestType
                0);                                                // errorCode
            SetupLockInfo(
                *request->MutableLockInfo(), // lockInfo
                2,                           // nodeId
                102,                         // handle
                5,                           // owner
                0,                           // offset
                32,                          // length
                0,                           // lockType
                {},                          // conflictedOwner
                {},                          // conflictedOffset
                {});                         // conflictedLength
        }

        {
            auto* request = FirstRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                60,                                               // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::AccessNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                7,                           // flags
                {},                          // mode
                2,                           // nodeId
                {},                          // handle
                {});                         // size
        }
    }

    void SetupSecondRecord()
    {
        SecondRecord.SetFileSystemId("fs2");

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                0,                                                // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                1,                           // newParentNodeId
                "test_node",                 // newNodeName
                {},                          // flags
                7,                           // mode
                2,                           // nodeId
                {},                          // handle
                64);                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                10,                                               // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::AccessNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                7,                           // flags
                {},                          // mode
                2,                           // nodeId
                {},                          // handle
                {});                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                           // request
                20,                                                 // timestmpMcs
                10,                                                 // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateHandle), // requestType
                0);                                                 // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                1,                           // parentNodeId
                "test_node",                 // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                3,                           // flags
                5,                           // mode
                2,                           // nodeId
                100,                         // handle
                64);                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                          // request
                30,                                                // timestmpMcs
                10,                                                // durationMcs
                static_cast<ui32>(EFileStoreRequest::SetNodeAttr), // requestType
                0);                                                // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                7,                           // flags
                5,                           // mode
                1,                           // nodeId
                101,                         // handle
                64);                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                           // request
                40,                                                 // timestmpMcs
                10,                                                 // durationMcs
                static_cast<ui32>(EFileStoreRequest::SetNodeXAttr), // requestType
                0);                                                 // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                "attr_name",                 // nodeName
                {},                          // newParentNodeId
                "attr_value",                // newNodeName
                7,                           // flags
                {},                          // mode
                1,                           // nodeId
                {},                          // handle
                16);                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                          // request
                50,                                                // timestmpMcs
                10,                                                // durationMcs
                static_cast<ui32>(EFileStoreRequest::GetNodeAttr), // requestType
                0);                                                // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                1,                           // parentNodeId
                "test_node",                 // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                5,                           // flags
                7,                           // mode
                2,                           // nodeId
                101,                         // handle
                16);                         // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                           // request
                60,                                                 // timestmpMcs
                10,                                                 // durationMcs
                static_cast<ui32>(EFileStoreRequest::GetNodeXAttr), // requestType
                0);                                                 // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                "attr_name",                 // nodeName
                {},                          // newParentNodeId
                "attr_value",                // newNodeName
                {},                          // flags
                {},                          // mode
                2,                           // nodeId
                {},                          // handle
                1);                          // size
        }

        {
            auto* request = SecondRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                            // request
                70,                                                  // timestmpMcs
                10,                                                  // durationMcs
                static_cast<ui32>(EFileStoreRequest::DestroyHandle), // requestType
                0);                                                  // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                {},                          // flags
                {},                          // mode
                2,                           // nodeId
                100,                         // handle
                {});                         // size
        }
    }

    void SetupThirdRecord()
    {
        ThirdRecord.SetFileSystemId("fs3");

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                         // request
                0,                                                // timestmpMcs
                10,                                               // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateNode), // requestType
                0);                                               // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                {},                          // parentNodeId
                {},                          // nodeName
                1,                           // newParentNodeId
                "test_node",                 // newNodeName
                {},                          // flags
                7,                           // mode
                2,                           // nodeId
                {},                          // handle
                64);                         // size
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                           // request
                10,                                                 // timestmpMcs
                10,                                                 // durationMcs
                static_cast<ui32>(EFileStoreRequest::CreateHandle), // requestType
                0);                                                 // errorCode
            SetupNodeInfo(
                *request->MutableNodeInfo(), // nodeInfo
                1,                           // parentNodeId
                "test_node",                 // nodeName
                {},                          // newParentNodeId
                {},                          // newNodeName
                3,                           // flags
                5,                           // mode
                2,                           // nodeId
                100,                         // handle
                64);                         // size
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                          // request
                20,                                                // timestmpMcs
                10,                                                // durationMcs
                static_cast<ui32>(EFileStoreRequest::AcquireLock), // requestType
                0);                                                // errorCode
            SetupLockInfo(
                *request->MutableLockInfo(), // lockInfo
                2,                           // nodeId
                100,                         // handle
                5,                           // owner
                0,                           // offset
                32,                          // length
                0,                           // lockType
                {},                          // conflictedOwner
                {},                          // conflictedOffset
                {});                         // conflictedLength
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                       // request
                30,                                             // timestmpMcs
                10,                                             // durationMcs
                static_cast<ui32>(EFileStoreRequest::TestLock), // requestType
                0);                                             // errorCode
            SetupLockInfo(
                *request->MutableLockInfo(), // lockInfo
                2,                           // nodeId
                100,                         // handle
                5,                           // owner
                0,                           // offset
                32,                          // length
                0,                           // lockType
                1,                           // conflictedOwner
                0,                           // conflictedOffset
                16);                         // conflictedLength
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                       // request
                40,                                             // timestmpMcs
                10,                                             // durationMcs
                static_cast<ui32>(EFileStoreRequest::ReadData), // requestType
                0);                                             // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                2,                     // nodeId
                100,                   // handle
                0,                     // offset
                32);                   // length
            SetupRange(
                *request->AddRanges(), // rangeInfo
                1,                     // nodeId
                100,                   // handle
                32,                    // offset
                32);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                       // request
                50,                                                             // timestmpMcs
                10,                                                             // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::ReadBlob), // requestType
                0);                                                             // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                1,                     // nodeId
                {},                    // handle
                0,                     // offset
                64);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                        // request
                60,                                              // timestmpMcs
                10,                                              // durationMcs
                static_cast<ui32>(EFileStoreRequest::WriteData), // requestType
                0);                                              // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                1,                     // nodeId
                100,                   // handle
                0,                     // offset
                32);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                        // request
                70,                                                              // timestmpMcs
                10,                                                              // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::WriteBlob), // requestType
                0);                                                              // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                2,                     // nodeId
                {},                    // handle
                0,                     // offset
                64);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                      // request
                80,                                                            // timestmpMcs
                10,                                                            // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::Cleanup), // requestType
                0);                                                            // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                2,                     // nodeId
                {},                    // handle
                0,                     // offset
                64);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                         // request
                90,                                                               // timestmpMcs
                10,                                                               // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::Compaction), // requestType
                0);                                                               // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                2,                     // nodeId
                {},                    // handle
                0,                     // offset
                64);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                             // request
                100,                                                                  // timestmpMcs
                10,                                                                   // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage), // requestType
                0);                                                                   // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                1,                     // nodeId
                100,                   // handle
                0,                     // offset
                64);                   // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                                            // request
                110,                                                                 // timestmpMcs
                10,                                                                  // durationMcs
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::DeleteGarbage), // requestType
                0);                                                                  // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                2,                     // nodeId
                {},                    // handle
                1,                     // offset
                0);                    // length
        }

        {
            auto* request = ThirdRecord.AddRequests();
            SetupRequestMainInfo(
                *request,                                        // request
                120,                                             // timestmpMcs
                10,                                              // durationMcs
                static_cast<ui32>(EFileStoreRequest::WriteData), // requestType
                0);                                              // errorCode
            SetupRange(
                *request->AddRanges(), // rangeInfo
                1,                     // nodeId
                100,                   // handle
                32,                    // offset
                32);                   // length
        }
    }

    void SetupRequestMainInfo(
        NProto::TProfileLogRequestInfo& request,
        ui64 timestampMcs,
        ui64 durationMcs,
        ui32 requestType,
        ui32 errorCode)
    {
        request.SetTimestampMcs(timestampMcs);
        request.SetDurationMcs(durationMcs);
        request.SetRequestType(requestType);
        request.SetErrorCode(errorCode);
    }

    void SetupLockInfo(
        NProto::TProfileLogLockInfo& lockInfo,
        const TMaybe<ui64>& nodeId,
        const TMaybe<ui64>& handle,
        const TMaybe<ui64>& owner,
        const TMaybe<ui64>& offset,
        const TMaybe<ui64>& length,
        const TMaybe<ui32>& type,
        const TMaybe<ui64>& conflictedOwner,
        const TMaybe<ui64>& conflictedOffset,
        const TMaybe<ui64>& conflictedLength)
    {
#define SETUP_FIELD(internal_name, name)                                       \
    if (internal_name.Defined()) {                                             \
        lockInfo.Set##name(internal_name.GetRef());                            \
    }                                                                          \
// SETUP_FIELD

        SETUP_FIELD(nodeId, NodeId);
        SETUP_FIELD(handle, Handle);
        SETUP_FIELD(owner, Owner);
        SETUP_FIELD(offset, Offset);
        SETUP_FIELD(length, Length);
        SETUP_FIELD(type, Type);
        SETUP_FIELD(conflictedOwner, ConflictedOwner);
        SETUP_FIELD(conflictedOffset, ConflictedOffset);
        SETUP_FIELD(conflictedLength, ConflictedLength);

#undef SETUP_FIELD
    }

    void SetupNodeInfo(
        NProto::TProfileLogNodeInfo& nodeInfo,
        const TMaybe<ui64>& parentNodeId,
        const TMaybe<TString>& nodeName,
        const TMaybe<ui64>& newParentNodeId,
        const TMaybe<TString>& newNodeName,
        const TMaybe<ui32>& flags,
        const TMaybe<ui32>& mode,
        const TMaybe<ui64>& nodeId,
        const TMaybe<ui64>& handle,
        const TMaybe<ui64>& size)
    {
#define SETUP_FIELD(internal_name, name)                                       \
    if (internal_name.Defined()) {                                             \
        nodeInfo.Set##name(internal_name.GetRef());                            \
    }                                                                          \
// SETUP_FIELD

        SETUP_FIELD(parentNodeId, ParentNodeId);
        SETUP_FIELD(nodeName, NodeName);
        SETUP_FIELD(newParentNodeId, NewParentNodeId);
        SETUP_FIELD(newNodeName, NewNodeName);
        SETUP_FIELD(flags, Flags);
        SETUP_FIELD(mode, Mode);
        SETUP_FIELD(nodeId, NodeId);
        SETUP_FIELD(handle, Handle);
        SETUP_FIELD(size, Size);

#undef SETUP_FIELD
    }

    void SetupRange(
        NProto::TProfileLogBlockRange& range,
        const TMaybe<ui64>& nodeId,
        const TMaybe<ui64>& handle,
        const TMaybe<ui64>& offset,
        const TMaybe<ui64>& bytes)
    {
#define SETUP_FIELD(internal_name, name)                                       \
    if (internal_name.Defined()) {                                             \
        range.Set##name(internal_name.GetRef());                               \
    }                                                                          \
// SETUP_FIELD

        SETUP_FIELD(nodeId, NodeId);
        SETUP_FIELD(handle, Handle);
        SETUP_FIELD(offset, Offset);
        SETUP_FIELD(bytes, Bytes);

#undef SETUP_FIELD
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestFilterTest)
{
    Y_UNIT_TEST_F(ShouldAcceptCompleteRecord, TEnv)
    {
        auto filter = CreateRequestFilterAccept();

        UNIT_ASSERT_VALUES_EQUAL(
            FirstRecord.ShortDebugString(),
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            SecondRecord.ShortDebugString(),
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            ThirdRecord.ShortDebugString(),
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByFileSystemIdRecord, TEnv)
    {
        auto filter = CreateRequestFilterByFileSystemId(
            CreateRequestFilterAccept(),
            "fs1");

        UNIT_ASSERT_VALUES_EQUAL(
            FirstRecord.ShortDebugString(),
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByNodeIdRecord, TEnv)
    {
        auto filter = CreateRequestFilterByNodeId(
            CreateRequestFilterAccept(),
            2);

        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs1\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 101 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 41 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 102 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } }",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs2\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 "
            "NodeName: \"test_node\" Flags: 3 Mode: 5 NodeId: 2 "
            "Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 33 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 "
            "NodeName: \"test_node\" Flags: 5 Mode: 7 NodeId: 2 "
            "Handle: 101 Size: 16 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 35 "
            "ErrorCode: 0 NodeInfo { NodeName: \"attr_name\" "
            "NewNodeName: \"attr_value\" NodeId: 2 Size: 1 } } "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 39 "
            "ErrorCode: 0 NodeInfo { NodeId: 2 Handle: 100 } }",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByHandleRecord, TEnv)
    {
        auto filter = CreateRequestFilterByHandle(
            CreateRequestFilterAccept(),
            100);

        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs1\" "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } }",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs2\" "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 39 "
            "ErrorCode: 0 NodeInfo { NodeId: 2 Handle: 100 } }",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 0 Bytes: 32 } } "
            "Requests { TimestampMcs: 120 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByRequestTypeRecord, TEnv)
    {
        auto filter = CreateRequestFilterByRequestType(
            CreateRequestFilterAccept(),
            {static_cast<ui32>(EFileStoreRequest::AccessNode)});

        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs1\" "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } }",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs2\" "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } }",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByRequestType(
            CreateRequestFilterAccept(),
            {static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage)});

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 100 DurationMcs: 10 RequestType: 10006 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 0 Bytes: 64 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByRangeRecord, TEnv) {
        auto filter = CreateRequestFilterByRange(
            CreateRequestFilterAccept(),
            16,
            48,
            4);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 10008 "
            "ErrorCode: 0 Ranges { NodeId: 1 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 0 Bytes: 32 } } "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 10009 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 120 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
        filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByRange(
            CreateRequestFilterAccept(),
            96,
            16,
            4);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByRange(
            CreateRequestFilterAccept(),
            64,
            16,
            4);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByRange(
            CreateRequestFilterAccept(),
            63,
            5,
            4);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 10008 "
            "ErrorCode: 0 Ranges { NodeId: 1 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 10009 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 120 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterBySinceRecord, TEnv)
    {
        auto filter = CreateRequestFilterSince(
            CreateRequestFilterAccept(),
            70);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs2\" "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 39 "
            "ErrorCode: 0 NodeInfo { NodeId: 2 Handle: 100 } }",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 70 DurationMcs: 10 RequestType: 10009 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 100 DurationMcs: 10 RequestType: 10006 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 110 DurationMcs: 10 RequestType: 10007 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 1 Bytes: 0 } } "
            "Requests { TimestampMcs: 120 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterByUntilRecord, TEnv)
    {
        auto filter = CreateRequestFilterUntil(
            CreateRequestFilterAccept(),
            0);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterUntil(
            CreateRequestFilterAccept(),
            70);

        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs1\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 101 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 28 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "NewParentNodeId: 1 NewNodeName: \"test_node_2\" } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 41 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 102 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } }",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs2\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 29 "
            "ErrorCode: 0 NodeInfo { Flags: 7 NodeId: 2 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 32 "
            "ErrorCode: 0 NodeInfo { Flags: 7 Mode: 5 NodeId: 1 Handle: 101 "
            "Size: 64 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 34 "
            "ErrorCode: 0 NodeInfo { NodeName: \"attr_name\" "
            "NewNodeName: \"attr_value\" Flags: 7 NodeId: 1 Size: 16 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 33 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 5 Mode: 7 NodeId: 2 Handle: 101 Size: 16 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 35 "
            "ErrorCode: 0 NodeInfo { NodeName: \"attr_name\" "
            "NewNodeName: \"attr_value\" NodeId: 2 Size: 1 } }",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 50 DurationMcs: 10 RequestType: 10008 "
            "ErrorCode: 0 Ranges { NodeId: 1 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 60 DurationMcs: 10 RequestType: 44 "
            "ErrorCode: 0 Ranges { NodeId: 1 Handle: 100 Offset: 0 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }

    Y_UNIT_TEST_F(ShouldFilterRecordWithPipeline, TEnv)
    {
        auto filter = CreateRequestFilterByFileSystemId(
            CreateRequestFilterAccept(),
            "fs3");
        filter = CreateRequestFilterByNodeId(std::move(filter), 2);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 0 DurationMcs: 10 RequestType: 26 "
            "ErrorCode: 0 NodeInfo { NewParentNodeId: 1 "
            "NewNodeName: \"test_node\" Mode: 7 NodeId: 2 Size: 64 } } "
            "Requests { TimestampMcs: 10 DurationMcs: 10 RequestType: 38 "
            "ErrorCode: 0 NodeInfo { ParentNodeId: 1 NodeName: \"test_node\" "
            "Flags: 3 Mode: 5 NodeId: 2 Handle: 100 Size: 64 } } "
            "Requests { TimestampMcs: 20 DurationMcs: 10 RequestType: 40 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 } } "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterSince(std::move(filter), 25);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } } "
            "Requests { TimestampMcs: 80 DurationMcs: 10 RequestType: 10004 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } } "
            "Requests { TimestampMcs: 90 DurationMcs: 10 RequestType: 10003 "
            "ErrorCode: 0 Ranges { NodeId: 2 Offset: 0 Bytes: 64 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterUntil(std::move(filter), 65);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByHandle(std::move(filter), 100);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 30 DurationMcs: 10 RequestType: 42 "
            "ErrorCode: 0 LockInfo { NodeId: 2 Handle: 100 Owner: 5 Offset: 0 "
            "Length: 32 Type: 0 ConflictedOwner: 1 ConflictedOffset: 0 "
            "ConflictedLength: 16 } } "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());

        auto filterByType = CreateRequestFilterByRequestType(
            filter,
            {static_cast<ui32>(EFileStoreRequest::ReadData)});

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filterByType->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filterByType->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filterByType->GetFilteredRecord(ThirdRecord).ShortDebugString());

        auto filterByRange = CreateRequestFilterByRange(
            std::move(filterByType),
            0,
            16,
            1);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filterByRange->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filterByRange->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "FileSystemId: \"fs3\" "
            "Requests { TimestampMcs: 40 DurationMcs: 10 RequestType: 43 "
            "ErrorCode: 0 Ranges { NodeId: 2 Handle: 100 Offset: 0 Bytes: 32 } "
            "Ranges { NodeId: 1 Handle: 100 Offset: 32 Bytes: 32 } }",
            filterByRange->GetFilteredRecord(ThirdRecord).ShortDebugString());

        filter = CreateRequestFilterByRange(
            std::move(filterByRange),
            64,
            1,
            1);

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(FirstRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(SecondRecord).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            filter->GetFilteredRecord(ThirdRecord).ShortDebugString());
    }
}

}   // namespace NCloud::NFileStore
