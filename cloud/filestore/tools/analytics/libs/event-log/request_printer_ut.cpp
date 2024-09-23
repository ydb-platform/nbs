#include "request_printer.h"
#include "cloud/filestore/dump.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/profile_log_events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv
    : public NUnitTest::TBaseFixture
{
    NProto::TProfileLogRequestInfo Request;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Request.SetTimestampMcs(10);
        Request.SetDurationMcs(20);
        Request.SetErrorCode(0);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestPrinterTest)
{
    Y_UNIT_TEST_F(ShouldPrintRequestInfoForDefaultRequestType, TEnv)
    {
        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::CreateNode));

        printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetParentNodeId(10);
        nodeInfo->SetNodeName("name_1");
        nodeInfo->SetNewParentNodeId(20);
        nodeInfo->SetNewNodeName("name_2");
        nodeInfo->SetFlags(5);
        nodeInfo->SetMode(7);
        nodeInfo->SetNodeId(30);
        nodeInfo->SetHandle(40);
        nodeInfo->SetSize(50);
        nodeInfo->SetType(2);

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, new_parent_node_id=20, "
            "new_node_name=name_2, flags=5, mode=7, node_id=30, handle=40, size=50, type=2}",
            printer->DumpInfo(Request));

        nodeInfo->ClearNewParentNodeId();
        nodeInfo->ClearNewNodeName();

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}",
            printer->DumpInfo(Request));

        auto* lockInfo = Request.MutableLockInfo();
        lockInfo->SetNodeId(100);
        lockInfo->SetHandle(110);
        lockInfo->SetOwner(120);
        lockInfo->SetOffset(130);
        lockInfo->SetLength(140);
        lockInfo->SetType(0);
        lockInfo->SetConflictedOwner(220);
        lockInfo->SetConflictedOffset(230);
        lockInfo->SetConflictedLength(240);

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}\t{node_id=100, handle=110, owner=120, offset=130, "
            "length=140, type=E_SHARED, conflicted_owner=220, conflicted_offset=230, "
            "conflicted_length=240}",
            printer->DumpInfo(Request));

        lockInfo->SetType(1);
        lockInfo->ClearConflictedOwner();
        lockInfo->ClearConflictedOffset();
        lockInfo->ClearConflictedLength();
        lockInfo->ClearOwner();

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}\t{node_id=100, handle=110, offset=130, length=140, "
            "type=E_EXCLUSIVE}",
            printer->DumpInfo(Request));

        lockInfo->SetType(2);
        for (ui32 i = 0; i < 2; ++i) {
            auto* range = Request.AddRanges();
            range->SetNodeId(300 + i);
            range->SetHandle(305 + i);
            range->SetOffset(310 + i);
            range->SetBytes(315 + i);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}\t{node_id=100, handle=110, offset=130, length=140, "
            "type=Unknown}\t[{node_id=300, handle=305, offset=310, bytes=315}, "
            "{node_id=301, handle=306, offset=311, bytes=316}]",
            printer->DumpInfo(Request));

        Request.MutableRanges()->RemoveLast();

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}\t{node_id=100, handle=110, offset=130, length=140, "
            "type=Unknown}\t[{node_id=300, handle=305, offset=310, bytes=315}]",
            printer->DumpInfo(Request));

        Request.ClearLockInfo();

        UNIT_ASSERT_VALUES_EQUAL(
            "{parent_node_id=10, node_name=name_1, flags=5, mode=7, node_id=30, "
            "handle=40, size=50, type=2}\t[{node_id=300, handle=305, offset=310, bytes=315}]",
            printer->DumpInfo(Request));

        Request.ClearNodeInfo();

        UNIT_ASSERT_VALUES_EQUAL(
            "[{node_id=300, handle=305, offset=310, bytes=315}]",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForAccessNodeRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::AccessNode));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetNodeId(10);
        nodeInfo->SetFlags(7);

        UNIT_ASSERT_VALUES_EQUAL(
            "{mask=7, node_id=10}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForSetNodeXAttrRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::SetNodeXAttr));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetNodeId(10);
        nodeInfo->SetNodeName("attribute");
        nodeInfo->SetNewNodeName("value");
        nodeInfo->SetFlags(7);
        nodeInfo->SetSize(128);

        UNIT_ASSERT_VALUES_EQUAL(
            "{attr_name=attribute, attr_value=value, flags=7, node_id=10, version=128}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForGetNodeXAttrRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::GetNodeXAttr));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetNodeId(10);
        nodeInfo->SetNodeName("attribute");
        nodeInfo->SetNewNodeName("value");
        nodeInfo->SetSize(128);

        UNIT_ASSERT_VALUES_EQUAL(
            "{attr_name=attribute, attr_value=value, node_id=10, version=128}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForRemoveNodeXAttrRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::RemoveNodeXAttr));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetParentNodeId(10);
        nodeInfo->SetNodeName("attribute");

        UNIT_ASSERT_VALUES_EQUAL(
            "{node_id=10, attr_name=attribute}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForCreateCheckpointRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::CreateCheckpoint));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetNodeId(10);
        nodeInfo->SetNodeName("checkpoint");

        UNIT_ASSERT_VALUES_EQUAL(
            "{checkpoint_id=checkpoint, node_id=10}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForDestroyCheckpointRequestType, TEnv)
    {
        Request.SetRequestType(static_cast<ui32>(EFileStoreRequest::DestroyCheckpoint));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetNodeName("checkpoint");

        UNIT_ASSERT_VALUES_EQUAL(
            "{checkpoint_id=checkpoint}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForFlushRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NFuse::EFileStoreFuseRequest::Flush));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetMode(1);
        nodeInfo->SetNodeId(123);
        nodeInfo->SetHandle(456);

        UNIT_ASSERT_VALUES_EQUAL(
            "{data_only=1, node_id=123, handle=456}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForFsyncRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NFuse::EFileStoreFuseRequest::Fsync));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* nodeInfo = Request.MutableNodeInfo();
        nodeInfo->SetMode(0);

        UNIT_ASSERT_VALUES_EQUAL(
            "{data_only=0}",
            printer->DumpInfo(Request));

        nodeInfo->SetMode(1);

        UNIT_ASSERT_VALUES_EQUAL(
            "{data_only=1}",
            printer->DumpInfo(Request));

        nodeInfo->SetNodeId(123);

        UNIT_ASSERT_VALUES_EQUAL(
            "{data_only=1, node_id=123}",
            printer->DumpInfo(Request));

        nodeInfo->SetHandle(456);

        UNIT_ASSERT_VALUES_EQUAL(
            "{data_only=1, node_id=123, handle=456}",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForCollectGarbageRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* range = Request.AddRanges();
        range->SetNodeId(123);
        range->SetHandle(456);
        range->SetOffset(32);
        range->SetBytes(64);

        UNIT_ASSERT_VALUES_EQUAL(
            "[{current_collect_commid_id=123, last_collect_commit_id=456, "
            "new_blobs=32, garbage_blobs=64}]",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForDeleteGarbageRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::DeleteGarbage));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        auto* range = Request.AddRanges();
        range->SetNodeId(123);
        range->SetOffset(32);
        range->SetBytes(64);

        UNIT_ASSERT_VALUES_EQUAL(
            "[{collect_commit_id=123, new_blobs=32, garbage_blobs=64}]",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForReadBlobRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::ReadBlob));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        for (ui32 i = 0; i < 3; ++i) {
            auto* range = Request.AddRanges();
            range->SetNodeId(i);
            range->SetOffset(32 * i);
            range->SetBytes(32);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "[{commit_id=0, offset=0, bytes=32}, "
            "{commit_id=1, offset=32, bytes=32}, "
            "{commit_id=2, offset=64, bytes=32}]",
            printer->DumpInfo(Request));
    }

    Y_UNIT_TEST_F(ShouldPrintRequestInfoForWriteBlobRequestType, TEnv)
    {
        Request.SetRequestType(
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::WriteBlob));

        auto printer = CreateRequestPrinter(Request.GetRequestType());
        UNIT_ASSERT_VALUES_EQUAL("{no_info}", printer->DumpInfo(Request));

        for (ui32 i = 0; i < 3; ++i) {
            auto* range = Request.AddRanges();
            range->SetNodeId(i);
            range->SetOffset(32 * i);
            range->SetBytes(32);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "[{commit_id=0, offset=0, bytes=32}, "
            "{commit_id=1, offset=32, bytes=32}, "
            "{commit_id=2, offset=64, bytes=32}]",
            printer->DumpInfo(Request));
    }
}

}   // namespace NCloud::NFileStore
