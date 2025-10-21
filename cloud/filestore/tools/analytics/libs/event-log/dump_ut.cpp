#include "dump.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDumpTest)
{
    Y_UNIT_TEST(ShouldGetItemOrderByTimestamp)
    {
        NProto::TProfileLogRecord record;
        record.SetFileSystemId("fs");

        record.MutableRequests();
        record.AddRequests()->SetTimestampMcs(10);
        record.AddRequests()->SetTimestampMcs(5);
        record.AddRequests()->SetTimestampMcs(20);
        record.AddRequests()->SetTimestampMcs(15);

        const auto requestOrder = GetItemOrder(record);

        UNIT_ASSERT_VALUES_EQUAL(4, requestOrder.size());
        UNIT_ASSERT_VALUES_EQUAL(1, requestOrder[0]);
        UNIT_ASSERT_VALUES_EQUAL(0, requestOrder[1]);
        UNIT_ASSERT_VALUES_EQUAL(3, requestOrder[2]);
        UNIT_ASSERT_VALUES_EQUAL(2, requestOrder[3]);
    }

    Y_UNIT_TEST(ShouldGetRequestNameByRequestType)
    {
#define TEST_PUBLIC_API(name, ...)                                             \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        #name,                                                                 \
        RequestName(static_cast<ui32>(EFileStoreRequest::name)));              \
// TEST_PUBLIC_API

        FILESTORE_REQUESTS(TEST_PUBLIC_API);

#undef TEST_PUBLIC_API

#define TEST_SYSTEM_API(name, ...)                                             \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        #name,                                                                 \
        RequestName(                                                           \
            static_cast<ui32>(NStorage::EFileStoreSystemRequest::name)));      \
// TEST_SYSTEM_API

        FILESTORE_SYSTEM_REQUESTS(TEST_SYSTEM_API);

#undef TEST_SYSTEM_API


#define TEST_FUSE_API(name, ...)                                               \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        #name,                                                                 \
        RequestName(                                                           \
            static_cast<ui32>(NFuse::EFileStoreFuseRequest::name)));           \
// TEST_FUSE_API

        FILESTORE_FUSE_REQUESTS(TEST_FUSE_API);

#undef TEST_FUSE_API

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "Unknown-" << static_cast<ui32>(EFileStoreRequest::MAX),
            RequestName(static_cast<ui32>(EFileStoreRequest::MAX)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "Unknown-"
                << static_cast<ui32>(NStorage::EFileStoreSystemRequest::MIN),
            RequestName(
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::MIN)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "Unknown-"
                << static_cast<ui32>(NStorage::EFileStoreSystemRequest::MAX),
            RequestName(
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::MAX)));
    }

    Y_UNIT_TEST(ShouldGetRequestTypes)
    {
        const auto requests = GetRequestTypes();

        UNIT_ASSERT_VALUES_EQUAL(74, requests.size());

        ui32 index = 0;
#define TEST_REQUEST_TYPE(id, name)                                            \
    UNIT_ASSERT_VALUES_EQUAL(id, requests[index].Id);                          \
    UNIT_ASSERT_VALUES_EQUAL(#name, requests[index].Name);                     \
    ++index;
// TEST_REQUEST_TYPE


        // Public
        TEST_REQUEST_TYPE(0, Ping);
        TEST_REQUEST_TYPE(1, PingSession);
        TEST_REQUEST_TYPE(2, CreateFileStore);
        TEST_REQUEST_TYPE(3, DestroyFileStore);
        TEST_REQUEST_TYPE(4, AlterFileStore);
        TEST_REQUEST_TYPE(5, ResizeFileStore);
        TEST_REQUEST_TYPE(6, DescribeFileStoreModel);
        TEST_REQUEST_TYPE(7, GetFileStoreInfo);
        TEST_REQUEST_TYPE(8, ListFileStores);
        TEST_REQUEST_TYPE(9, CreateSession);
        TEST_REQUEST_TYPE(10, DestroySession);
        TEST_REQUEST_TYPE(11, AddClusterNode);
        TEST_REQUEST_TYPE(12, RemoveClusterNode);
        TEST_REQUEST_TYPE(13, ListClusterNodes);
        TEST_REQUEST_TYPE(14, AddClusterClients);
        TEST_REQUEST_TYPE(15, RemoveClusterClients);
        TEST_REQUEST_TYPE(16, ListClusterClients);
        TEST_REQUEST_TYPE(17, UpdateCluster);
        TEST_REQUEST_TYPE(18, CreateCheckpoint);
        TEST_REQUEST_TYPE(19, DestroyCheckpoint);
        TEST_REQUEST_TYPE(20, ExecuteAction);
        TEST_REQUEST_TYPE(21, StatFileStore);
        TEST_REQUEST_TYPE(22, SubscribeSession);
        TEST_REQUEST_TYPE(23, GetSessionEvents);
        TEST_REQUEST_TYPE(24, ResetSession);
        TEST_REQUEST_TYPE(25, ResolvePath);
        TEST_REQUEST_TYPE(26, CreateNode);
        TEST_REQUEST_TYPE(27, UnlinkNode);
        TEST_REQUEST_TYPE(28, RenameNode);
        TEST_REQUEST_TYPE(29, AccessNode);
        TEST_REQUEST_TYPE(30, ListNodes);
        TEST_REQUEST_TYPE(31, ReadLink);
        TEST_REQUEST_TYPE(32, SetNodeAttr);
        TEST_REQUEST_TYPE(33, GetNodeAttr);
        TEST_REQUEST_TYPE(34, SetNodeXAttr);
        TEST_REQUEST_TYPE(35, GetNodeXAttr);
        TEST_REQUEST_TYPE(36, ListNodeXAttr);
        TEST_REQUEST_TYPE(37, RemoveNodeXAttr);
        TEST_REQUEST_TYPE(38, CreateHandle);
        TEST_REQUEST_TYPE(39, DestroyHandle);
        TEST_REQUEST_TYPE(40, AcquireLock);
        TEST_REQUEST_TYPE(41, ReleaseLock);
        TEST_REQUEST_TYPE(42, TestLock);
        TEST_REQUEST_TYPE(43, ReadData);
        TEST_REQUEST_TYPE(44, WriteData);
        TEST_REQUEST_TYPE(45, AllocateData);
        TEST_REQUEST_TYPE(46, Fsync);
        TEST_REQUEST_TYPE(47, FsyncDir);
        TEST_REQUEST_TYPE(48, GetSessionEventsStream);
        TEST_REQUEST_TYPE(49, StartEndpoint);
        TEST_REQUEST_TYPE(50, StopEndpoint);
        TEST_REQUEST_TYPE(51, ListEndpoints);
        TEST_REQUEST_TYPE(52, KickEndpoint);
        TEST_REQUEST_TYPE(53, DescribeData);
        TEST_REQUEST_TYPE(54, GenerateBlobIds);
        TEST_REQUEST_TYPE(55, AddData);
        TEST_REQUEST_TYPE(56, ReadBlob);
        TEST_REQUEST_TYPE(57, WriteBlob);
        TEST_REQUEST_TYPE(58, ToggleServiceState);

        // Fuse
        TEST_REQUEST_TYPE(1001, Flush);
        TEST_REQUEST_TYPE(1002, Fsync);
        TEST_REQUEST_TYPE(1003, FsyncDir);

        // Tablet
        TEST_REQUEST_TYPE(10001, Flush);
        TEST_REQUEST_TYPE(10002, FlushBytes);
        TEST_REQUEST_TYPE(10003, Compaction);
        TEST_REQUEST_TYPE(10004, Cleanup);
        TEST_REQUEST_TYPE(10005, TrimBytes);
        TEST_REQUEST_TYPE(10006, CollectGarbage);
        TEST_REQUEST_TYPE(10007, DeleteGarbage);
        TEST_REQUEST_TYPE(10008, ReadBlob);
        TEST_REQUEST_TYPE(10009, WriteBlob);
        TEST_REQUEST_TYPE(10010, AddBlob);
        TEST_REQUEST_TYPE(10011, TruncateRange);
        TEST_REQUEST_TYPE(10012, ZeroRange);

#undef TEST_REQUEST_TYPE
    }

    Y_UNIT_TEST(ShouldDumpRequestMainInfo)
    {
        NProto::TProfileLogRecord record;
        record.SetFileSystemId("fs");

        {
            auto* req = record.AddRequests();
            req->SetTimestampMcs(10);
            req->SetDurationMcs(20);
            req->SetRequestType(static_cast<ui32>(EFileStoreRequest::ReadData));
            req->SetErrorCode(0);
        }

        {
            auto* req = record.AddRequests();
            req->SetTimestampMcs(50);
            req->SetDurationMcs(60);
            req->SetRequestType(
                static_cast<ui32>(NStorage::EFileStoreSystemRequest::Compaction));
            req->SetErrorCode(1);
        }

        TStringStream testStream;

        DumpRequest(record, 0, &testStream);
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:00.000010Z\tfs\tReadData\t0.000020s\tS_OK\t{no_info}\n",
            testStream.Str());

        DumpRequest(record, 1, &testStream);
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:00.000010Z\tfs\tReadData\t0.000020s\tS_OK\t{no_info}\n"
            "1970-01-01T00:00:00.000050Z\tfs\tCompaction\t0.000060s\tS_FALSE\t{no_info}\n",
            testStream.Str());
    }
}

}   // namespace NCloud::NFileStore
