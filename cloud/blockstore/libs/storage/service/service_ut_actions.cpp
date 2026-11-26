#include "service_ut.h"

#include "scan_disk_batch_actor.h"
#include "service_events_private.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_private.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>
#include <cloud/blockstore/libs/storage/protos_ydb/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>

#include <google/protobuf/util/json_util.h>

using TVolumeParamMap = ::google::protobuf::Map<
    TString,
    NCloud::NBlockStore::NProto::TUpdateVolumeParamsMapValue>;

bool operator==(const TVolumeParamMap& lhs,
                const TVolumeParamMap& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }

    return std::all_of(lhs.begin(), lhs.end(), [&](const auto& elem){
        const auto& a = elem.second;
        const auto& b = rhs.at(elem.first);
        return a.GetValue() == b.GetValue() && a.GetTtlMs() == b.GetTtlMs();
    });
}

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;
using namespace std::string_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCheckIntegrityResult = TEvBlobStorage::TEvCheckIntegrityResult;
using TScanDiskBlobResult = NPrivateProto::TScanDiskBlobResult;

constexpr ui32 ScanDiskBlobBlockCount = 1024;
constexpr ui32 ScanDiskBlobsPerPartition = 4;

NProto::TStorageServiceConfig MakeScanDiskBatchConfig(
    bool checksums,
    ui32 partitionCount)
{
    NProto::TStorageServiceConfig config;
    if (checksums) {
        config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
    }
    config.SetCleanupThreshold(1);

    if (partitionCount > 1) {
        // A stripe per blob, so that every blob lands in one partition.
        const ui64 bytesPerPartition = ScanDiskBlobsPerPartition *
                                       ScanDiskBlobBlockCount *
                                       DefaultBlockSize;
        config.SetMultipartitionVolumesEnabled(true);
        config.SetBytesPerPartition(bytesPerPartition);
        config.SetBytesPerPartitionSSD(bytesPerPartition);
        config.SetBytesPerStripe(ScanDiskBlobBlockCount * DefaultBlockSize);
        config.SetMaxPartitionsPerVolume(partitionCount);
    }

    return config;
}

TBlockRange64 MakeScanDiskBlobRange(ui32 blobIndex)
{
    return TBlockRange64::WithLength(
        blobIndex * ScanDiskBlobBlockCount,
        ScanDiskBlobBlockCount);
}

NPrivateProto::TScanDiskBatchRequest MakeScanDiskBatchRequest(ui32 count = 100)
{
    NPrivateProto::TScanDiskBatchRequest request;
    request.SetDiskId(DefaultDiskId);
    request.SetCount(count);
    return request;
}

TString ToJson(const google::protobuf::Message& message)
{
    TString json;
    google::protobuf::util::MessageToJsonString(message, &json);
    return json;
}

NPrivateProto::TScanDiskBatchResponse ParseScanDiskBatchResponse(
    const TEvServicePrivate::TEvScanDiskBatchResponse& response)
{
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetStatus(),
        response.GetErrorReason());
    return response.Record;
}

TLogoBlobID MakeLogoBlobId(const NPrivateProto::TLogoBlobID& blobId)
{
    return TLogoBlobID(blobId.GetRawX1(), blobId.GetRawX2(), blobId.GetRawX3());
}

ui32 GetNodeIndex(TTestActorRuntimeBase& runtime, const TActorId& actorId)
{
    ui32 nodeIndex = 0;
    while (runtime.GetNodeId(nodeIndex) != actorId.NodeId()) {
        ++nodeIndex;
    }
    return nodeIndex;
}

void ReplyToCheckIntegrity(
    TTestActorRuntimeBase& runtime,
    IEventHandle& request,
    TCheckIntegrityResult::EPlacementStatus placementStatus)
{
    auto result = std::make_unique<TCheckIntegrityResult>(NKikimrProto::OK);
    result->Id = request.Get<TEvBlobStorage::TEvCheckIntegrity>()->Id;
    result->PlacementStatus = placementStatus;
    result->DataStatus = TCheckIntegrityResult::DS_OK;
    result->DataInfo = "crafted";

    runtime.Send(
        new IEventHandle(
            request.Sender,
            request.Recipient,
            result.release(),
            0,   // flags
            request.Cookie),
        GetNodeIndex(runtime, request.Sender));
}

// Answers the checks of the given blobs with the given placement status and
// lets the other checks through.
auto MakeCheckIntegrityFilter(
    THashMap<TLogoBlobID, TCheckIntegrityResult::EPlacementStatus>&
        placementStatuses)
{
    return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
    {
        if (event->GetTypeRewrite() != TEvBlobStorage::EvCheckIntegrity) {
            return false;
        }

        const auto& blobId =
            event->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id;
        const auto* placementStatus = placementStatuses.FindPtr(blobId);
        if (!placementStatus) {
            return false;
        }

        ReplyToCheckIntegrity(runtime, *event, *placementStatus);
        return true;
    };
}

// Every check finds its blob lost, so that every live blob is reported.
bool ReplyThatBlobIsLost(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& event)
{
    if (event->GetTypeRewrite() != TEvBlobStorage::EvCheckIntegrity) {
        return false;
    }

    ReplyToCheckIntegrity(
        runtime,
        *event,
        TCheckIntegrityResult::PS_BLOB_IS_LOST);
    return true;
}

// Fails the requests of this partition the way a restarting one does.
auto MakeRejectPartitionFilter(ui32 partitionIndex, ui32& rejectedRequests)
{
    return [=, &rejectedRequests](
               TTestActorRuntimeBase& runtime,
               TAutoPtr<IEventHandle>& event)
    {
        if (event->GetTypeRewrite() != TEvVolume::EvScanDiskBatchRequest) {
            return false;
        }

        using TEv = TEvVolume::TEvScanDiskBatchRequest;
        if (event->Get<TEv>()->Record.GetPartitionIndex() != partitionIndex) {
            return false;
        }

        ++rejectedRequests;
        runtime.Send(
            new IEventHandle(
                event->Sender,
                event->Recipient,
                new TEvVolume::TEvScanDiskBatchResponse(
                    MakeError(E_REJECTED, "crafted error")),
                0,   // flags
                event->Cookie),
            GetNodeIndex(runtime, event->Sender));
        return true;
    };
}

////////////////////////////////////////////////////////////////////////////////

using TScanDiskRecord = NPrivateProto::TScanDiskRecord;
using TScanDiskStatus = NPrivateProto::TGetScanDiskStatusResponse;
using TScanDiskCounters = NPrivateProto::TScanDiskCounters;

NPrivateProto::TScanDiskRequest MakeScanDiskRequest()
{
    NPrivateProto::TScanDiskRequest request;
    request.SetDiskId(DefaultDiskId);
    request.SetBatchSize(1);
    return request;
}

template <typename T>
T ParseJson(const TString& json)
{
    T message;
    UNIT_ASSERT_C(
        google::protobuf::util::JsonStringToMessage(json, &message).ok(),
        json);
    return message;
}

TScanDiskCounters SumCounters(const TVector<TScanDiskRecord>& records)
{
    TScanDiskCounters total;
    const auto* descriptor = total.GetDescriptor();
    const auto* reflection = total.GetReflection();
    for (const auto& record: records) {
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            reflection->SetUInt64(
                &total,
                field,
                reflection->GetUInt64(total, field) +
                    reflection->GetUInt64(record.GetCounters(), field));
        }
    }
    return total;
}

TVector<TScanDiskBlobResult> GetBlobResults(
    const TVector<TScanDiskRecord>& records)
{
    TVector<TScanDiskBlobResult> results;
    for (const auto& record: records) {
        for (const auto& result: record.GetBlobResults()) {
            results.push_back(result);
        }
    }
    return results;
}

TVector<TLogoBlobID> GetReportedBlobIds(const TVector<TScanDiskRecord>& records)
{
    TVector<TLogoBlobID> blobIds;
    for (const auto& result: GetBlobResults(records)) {
        blobIds.push_back(MakeLogoBlobId(result.GetBlobId()));
    }
    Sort(blobIds);
    return blobIds;
}

ui64 GetCommitId(const TLogoBlobID& blobId)
{
    return MakeCommitId(blobId.Generation(), blobId.Step());
}

// What a caller saves: every record, and the status that completed the scan.
struct TScanDiskResult
{
    TVector<TScanDiskRecord> Records;
    TScanDiskStatus Status;
};

////////////////////////////////////////////////////////////////////////////////

struct TScanDiskBatchTestEnv
{
    TTestEnv Env;
    ui32 NodeIdx = 0;
    std::unique_ptr<TServiceClient> Service;
    TString SessionId;

    explicit TScanDiskBatchTestEnv(
        bool checksums = true,
        ui32 partitionCount = 1)
    {
        NodeIdx = SetupTestEnv(
            Env,
            MakeScanDiskBatchConfig(checksums, partitionCount));
        Service = std::make_unique<TServiceClient>(Env.GetRuntime(), NodeIdx);
        Service->CreateVolume(
            DefaultDiskId,
            partitionCount * ScanDiskBlobsPerPartition *
                ScanDiskBlobBlockCount);
        SessionId = Service->MountVolume()->Record.GetSessionId();
    }

    TTestActorRuntime& GetRuntime()
    {
        return Env.GetRuntime();
    }

    void WriteBlob(ui32 blobIndex, char fill)
    {
        Service->WriteBlocks(
            DefaultDiskId,
            MakeScanDiskBlobRange(blobIndex),
            SessionId,
            fill);
    }

    // Runs the batch actor the way the scan actor does.
    TActorId SendScanDiskBatch(
        const NPrivateProto::TScanDiskBatchRequest& request)
    {
        auto& runtime = GetRuntime();
        const auto owner = runtime.AllocateEdgeActor(NodeIdx);
        const auto actorId = runtime.Register(
            CreateScanDiskBatchActor(owner, request).release(),
            NodeIdx);
        // The batch actor relies on its deadline wakeups.
        runtime.EnableScheduleForActor(actorId);
        return owner;
    }

    TEvServicePrivate::TEvScanDiskBatchResponse::TPtr RecvScanDiskBatchResponse(
        const TActorId& owner)
    {
        return GetRuntime()
            .GrabEdgeEvent<TEvServicePrivate::TEvScanDiskBatchResponse>(owner);
    }

    NPrivateProto::TScanDiskBatchResponse ScanDiskBatch(
        const NPrivateProto::TScanDiskBatchRequest& request)
    {
        return ParseScanDiskBatchResponse(
            *RecvScanDiskBatchResponse(SendScanDiskBatch(request))->Get());
    }

    std::unique_ptr<TEvService::TEvExecuteActionResponse> SendScanDisk(
        const TString& input)
    {
        Service->SendExecuteActionRequest("scandisk", input);
        return Service->RecvExecuteActionResponse();
    }

    TString StartScanDisk(const NPrivateProto::TScanDiskRequest& request)
    {
        const auto response = SendScanDisk(ToJson(request));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());

        const auto operationId = ParseJson<NPrivateProto::TScanDiskResponse>(
                                     response->Record.GetOutput())
                                     .GetOperationId();
        UNIT_ASSERT(operationId);
        return operationId;
    }

    std::unique_ptr<TEvService::TEvExecuteActionResponse> SendGetScanDiskStatus(
        const TString& operationId,
        ui64 ackedRecordId)
    {
        NPrivateProto::TGetScanDiskStatusRequest request;
        request.SetOperationId(operationId);
        request.SetAckedRecordId(ackedRecordId);
        Service->SendExecuteActionRequest("getscandiskstatus", ToJson(request));
        return Service->RecvExecuteActionResponse();
    }

    TScanDiskStatus GetScanDiskStatus(
        const TString& operationId,
        ui64 ackedRecordId)
    {
        const auto response = SendGetScanDiskStatus(operationId, ackedRecordId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
        return ParseJson<TScanDiskStatus>(response->Record.GetOutput());
    }

    void CheckScanDiskNotFound(const TString& operationId)
    {
        const auto response = SendGetScanDiskStatus(operationId, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_NOT_FOUND,
            response->GetStatus(),
            response->GetErrorReason());
    }

    // Polls the way a caller does: saves the records in order, acknowledges
    // them, and checks that acknowledging the last one stops the scan.
    TScanDiskResult PollScanDiskToEnd(
        const TString& operationId,
        ui64 ackedRecordId = 0)
    {
        TScanDiskResult result;
        for (ui32 poll = 0;; ++poll) {
            UNIT_ASSERT_LT(poll, 1000);

            auto status = GetScanDiskStatus(operationId, ackedRecordId);
            for (const auto& record: status.GetRecords()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    ackedRecordId + 1,
                    record.GetRecordId());
                ackedRecordId = record.GetRecordId();
                result.Records.push_back(record);
            }

            if (status.GetIsCompleted()) {
                result.Status = std::move(status);
                break;
            }

            GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        const auto status = GetScanDiskStatus(operationId, ackedRecordId);
        UNIT_ASSERT(status.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(0, status.RecordsSize());
        CheckScanDiskNotFound(operationId);

        return result;
    }

    TScanDiskResult ScanDiskToEnd(
        const NPrivateProto::TScanDiskRequest& request)
    {
        return PollScanDiskToEnd(StartScanDisk(request));
    }

    ui64 GetPartitionTabletId()
    {
        const auto response = Service->ExecuteAction(
            "describevolume",
            TStringBuilder() << R"({"DiskId": ")" << DefaultDiskId << R"("})");

        NJson::TJsonValue description;
        UNIT_ASSERT(
            NJson::ReadJsonTree(response->Record.GetOutput(), &description));
        return description["Partitions"][0]["TabletId"].GetUIntegerRobust();
    }

    // Lists the live blobs the way the batch actor does and returns their ids.
    TVector<TLogoBlobID> ListBlobs()
    {
        auto& runtime = GetRuntime();

        TVector<TLogoBlobID> blobIds;
        bool dropChecks = true;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvScanDiskBatchResponse: {
                        using TEv = TEvVolume::TEvScanDiskBatchResponse;
                        for (const auto& blob:
                             event->Get<TEv>()->Record.GetBlobs()) {
                            blobIds.push_back(
                                LogoBlobIDFromLogoBlobID(blob.GetBlobId()));
                        }
                        break;
                    }
                    case TEvBlobStorage::EvCheckIntegrity:
                        return dropChecks;
                }
                return false;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetDeadlineMs(1000);
        ScanDiskBatch(request);

        dropChecks = false;
        runtime.SetEventFilter(TTestActorRuntime::DefaultFilterFunc);

        SortUnique(blobIds);
        return blobIds;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceActionsTest)
{
    Y_UNIT_TEST(ShouldForwardChangeStateRequestsToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        auto* disk = drState->Disks.FindPtr(DefaultDiskId);
        UNIT_ASSERT(disk);
        UNIT_ASSERT_VALUES_UNEQUAL(0, disk->Devices.size());
        const auto& device = disk->Devices[0];
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::DEVICE_STATE_ONLINE),
            static_cast<ui32>(device.GetState())
        );

        {
            NPrivateProto::TDiskRegistryChangeStateRequest request;
            auto* cds = request.MutableChangeDeviceState();
            cds->SetDeviceUUID(device.GetDeviceUUID());
            cds->SetState(static_cast<ui32>(NProto::DEVICE_STATE_ERROR));

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("DiskRegistryChangeState", buf);

            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());

            request.SetMessage("some message");
            buf.clear();
            google::protobuf::util::MessageToJsonString(request, &buf);

            service.ExecuteAction("DiskRegistryChangeState", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::DEVICE_STATE_ERROR),
            static_cast<ui32>(device.GetState())
        );

        {
            NPrivateProto::TDiskRegistryChangeStateRequest request;
            request.SetMessage("some message");
            auto* cds = request.MutableChangeDeviceState();
            cds->SetDeviceUUID("nosuchdevice");
            cds->SetState(static_cast<ui32>(NProto::DEVICE_STATE_ERROR));

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("DiskRegistryChangeState", buf);

            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }

        {
            NPrivateProto::TDiskRegistryChangeStateRequest request;
            request.SetMessage("some message");
            auto* cds = request.MutableChangeAgentState();
            cds->SetAgentId("someagent");
            cds->SetState(static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE));

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("DiskRegistryChangeState", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, drState->AgentStates.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "someagent",
            drState->AgentStates[0].first
        );
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::AGENT_STATE_UNAVAILABLE),
            static_cast<ui32>(drState->AgentStates[0].second)
        );
    }

    Y_UNIT_TEST(ShouldModifyVolumeTagsAndDescribeVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        {
            service.SendExecuteActionRequest("ModifyTags", "{");
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            // Incorrect ConfigVersion.
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(2);
            *request.AddTagsToAdd() = "a";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("ModifyTags", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->GetStatus());
        }

        {
            // Pass ConfigVersion.
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            *request.AddTagsToAdd() = "a";
            *request.AddTagsToAdd() = "b";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            // Do not pass ConfigVersion.
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "c";
            *request.AddTagsToRemove() = "b";
            *request.AddTagsToRemove() = "d";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = ",,,";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("ModifyTags", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            NPrivateProto::TDescribeVolumeRequest request;
            request.SetDiskId(DefaultDiskId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("DescribeVolume", buf);
            NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &pathDescr
            ).ok());
            const auto& config = pathDescr.GetVolumeConfig();
            UNIT_ASSERT_VALUES_EQUAL("a,c", config.GetTagsStr());
        }

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToRemove() = "a";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToRemove() = "c";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NPrivateProto::TDescribeVolumeRequest request;
            request.SetDiskId(DefaultDiskId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("DescribeVolume", buf);
            NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &pathDescr
            ).ok());
            const auto& config = pathDescr.GetVolumeConfig();
            UNIT_ASSERT_VALUES_EQUAL("", config.GetTagsStr());
        }

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = MakeTag("source-disk-id", "disk@id");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NPrivateProto::TDescribeVolumeRequest request;
            request.SetDiskId(DefaultDiskId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("DescribeVolume", buf);
            NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &pathDescr
            ).ok());
            const auto& config = pathDescr.GetVolumeConfig();
            UNIT_ASSERT_VALUES_EQUAL(
                "source-disk-id=disk@id",
                config.GetTagsStr());
        }

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToRemove() = MakeTag("source-disk-id", "disk@id");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NPrivateProto::TDescribeVolumeRequest request;
            request.SetDiskId(DefaultDiskId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("DescribeVolume", buf);
            NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &pathDescr
            ).ok());
            const auto& config = pathDescr.GetVolumeConfig();
            UNIT_ASSERT_VALUES_EQUAL("", config.GetTagsStr());
        }

        // Test AddTagsRequest and RemoveTagsRequest.
        {
            service.SendAddTagsRequest(
                DefaultDiskId,
                TVector{TString("keep"), MakeTag("source-disk-id", "disk@id")});
            auto response = service.RecvAddTagsResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            service.RemoveTags(
                DefaultDiskId,
                TVector{MakeTag("source-disk-id", "disk@id")});
            service.AddTags(DefaultDiskId, TVector{TString("tag")});
        }

        {
            NPrivateProto::TDescribeVolumeRequest request;
            request.SetDiskId(DefaultDiskId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto response = service.ExecuteAction("DescribeVolume", buf);
            NKikimrSchemeOp::TBlockStoreVolumeDescription pathDescr;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &pathDescr
            ).ok());
            const auto& config = pathDescr.GetVolumeConfig();
            UNIT_ASSERT_VALUES_EQUAL("keep,tag", config.GetTagsStr());
        }

        service.DestroyVolume();
    }

    Y_UNIT_TEST(ShouldNotAlterVolumeIfTagsNotChanged)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "a";
            *request.AddTagsToAdd() = "b";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            bool alterVolume = false;
            env.GetRuntime().SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    switch (event->GetTypeRewrite()) {
                        case TEvSSProxy::EvModifySchemeRequest: {
                            alterVolume = true;
                        }
                    }

                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "b";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);

            UNIT_ASSERT_VALUES_EQUAL(false, alterVolume);
        }
    }

    Y_UNIT_TEST(ShouldAllowToChangeBaseDiskId)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);
        service.CreateVolume("new-base-disk");

        UNIT_ASSERT_EQUAL(0, GetVolumeConfig(service, DefaultDiskId).GetBaseDiskTabletId());

        {
            NPrivateProto::TRebaseVolumeRequest rebaseReq;
            rebaseReq.SetDiskId(DefaultDiskId);
            rebaseReq.SetTargetBaseDiskId("new-base-disk");
            rebaseReq.SetConfigVersion(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(rebaseReq, &buf);
            service.ExecuteAction("rebasevolume", buf);
        }

        auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL("new-base-disk", volumeConfig.GetBaseDiskId());
        UNIT_ASSERT_UNEQUAL(0, volumeConfig.GetBaseDiskTabletId());
    }

    Y_UNIT_TEST(ShouldForwardDeleteCheckpointDataToVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);
        auto createCheckpointResponse = service.CreateCheckpoint(DefaultDiskId, "c1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, createCheckpointResponse->GetStatus());

        {
            NPrivateProto::TDeleteCheckpointDataRequest deleteCheckpointDataRequest;
            deleteCheckpointDataRequest.SetDiskId(DefaultDiskId);
            deleteCheckpointDataRequest.SetCheckpointId("c1");

            TString buf;
            google::protobuf::util::MessageToJsonString(deleteCheckpointDataRequest, &buf);
            auto executeResponse = service.ExecuteAction("deletecheckpointdata", buf);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, executeResponse->GetStatus());

            NPrivateProto::TDeleteCheckpointDataResponse deleteCheckpointDataResponse;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                executeResponse->Record.GetOutput(),
                &deleteCheckpointDataResponse
            ).ok());
            UNIT_ASSERT_VALUES_EQUAL(S_OK, deleteCheckpointDataResponse.GetError().GetCode());
        }

        auto deleteCheckpointResponse = service.DeleteCheckpoint(DefaultDiskId, "c1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, deleteCheckpointResponse->GetStatus());
    }

    Y_UNIT_TEST(ShouldForwardWritableStateRequestsToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        UNIT_ASSERT(!drState->WritableState);

        NPrivateProto::TDiskRegistrySetWritableStateRequest request;
        request.SetState(true);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("diskregistrysetwritablestate", buf);

        UNIT_ASSERT(drState->WritableState);
    }

    Y_UNIT_TEST(ShouldForwardSuspendSecureEraseRequestsToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        UNIT_ASSERT(!drState->SuspendSecureErase);

        NProto::TSetSuspendSecureEraseRequest request;
        request.SetSuspend(true);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("diskregistrysuspendsecureerase", buf);

        UNIT_ASSERT(drState->SuspendSecureErase);
    }

    Y_UNIT_TEST(ShouldForwardBackupRequestsToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        NProto::TBackupDiskRegistryStateRequest request;
        request.SetSource(NProto::BDRSS_MEMORY);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto response = service.ExecuteAction("backupdiskregistrystate", buf);

        NProto::TBackupDiskRegistryStateResponse proto;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            response->Record.GetOutput(),
            &proto
        ).ok());
        const auto& backup = proto.GetMemoryBackup();

        UNIT_ASSERT_VALUES_EQUAL(1, backup.DisksSize());
        UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, backup.GetDisks(0).GetDiskId());
    }

    Y_UNIT_TEST(ShouldForwardUpdatePlacementGroupSettingsRequestsToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreatePlacementGroup(
            "group-1",
            NProto::PLACEMENT_STRATEGY_SPREAD,
            0
        );

        NProto::TUpdatePlacementGroupSettingsRequest request;
        request.SetGroupId("group-1");
        request.SetConfigVersion(1);
        request.MutableSettings()->SetMaxDisksInGroup(100);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto executeResponse = service.ExecuteAction("updateplacementgroupsettings", buf);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, executeResponse->GetStatus());

        NProto::TBackupDiskRegistryStateResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            executeResponse->Record.GetOutput(),
            &response
        ).ok());

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
        const auto* group = drState->PlacementGroups.FindPtr("group-1");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(100, group->Settings.GetMaxDisksInGroup());
    }

    Y_UNIT_TEST(ShouldForwardMarkReplacementDeviceToDiskRegistry)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString deviceUUID = "device1";

        UNIT_ASSERT(!drState->DeviceReplacementUUIDs.contains(deviceUUID));

        NProto::TMarkReplacementDeviceRequest request;
        request.SetDeviceId(deviceUUID);
        request.SetIsReplacement(true);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto executeResponse =
            service.ExecuteAction("markreplacementdevice", buf);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, executeResponse->GetStatus());

        UNIT_ASSERT(drState->DeviceReplacementUUIDs.contains(deviceUUID));
    }

    Y_UNIT_TEST(ShouldRebindLocalVolumes)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol0");   // local
        service.CreateVolume("vol1");   // local
        service.CreateVolume("vol2");   // remote
        service.CreateVolume("vol3");   // not mounted

        service.MountVolume("vol0");
        service.MountVolume("vol1");
        service.MountVolume(
            "vol2",
            TString(),  // instanceId
            TString(),  // token
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE
        );

        TSet<TString> diskIds;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvChangeVolumeBindingRequest: {
                        auto* msg = event->Get<TEvService::TEvChangeVolumeBindingRequest>();
                        UNIT_ASSERT_VALUES_EQUAL(
                            static_cast<ui32>(TEvService::TChangeVolumeBindingRequest::EChangeBindingOp::RELEASE_TO_HIVE),
                            static_cast<ui32>(msg->Action)
                        );

                        diskIds.insert(msg->DiskId);
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        NPrivateProto::TRebindVolumesRequest request;
        request.SetBinding(2);  // REMOTE

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("rebindvolumes", buf);

        UNIT_ASSERT_VALUES_EQUAL(2, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL(diskIds.count("vol0"), 1);
        UNIT_ASSERT_VALUES_EQUAL(diskIds.count("vol1"), 1);
    }

    Y_UNIT_TEST(ShouldRebindLocalVolumesWithGentlePreemption)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllowGentlePreemptionForRebindVolumesAction(true);
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol0");   // local
        service.CreateVolume("vol1");   // local
        service.CreateVolume("vol2");   // remote
        service.CreateVolume("vol3");   // not mounted

        service.MountVolume("vol0");
        service.MountVolume("vol1");
        service.MountVolume(
            "vol2",
            TString(),   // instanceId
            TString(),   // token
            NProto::IPC_GRPC,
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE);

        TSet<TString> diskIds;

        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvChangeVolumeBindingRequest: {
                        auto* msg = event->Get<
                            TEvService::TEvChangeVolumeBindingRequest>();
                        UNIT_ASSERT_VALUES_EQUAL(
                            static_cast<ui32>(
                                TEvService::TChangeVolumeBindingRequest::
                                    EChangeBindingOp::RELEASE_TO_HIVE),
                            static_cast<ui32>(msg->Action));
                        UNIT_ASSERT(msg->UseGentlePreemption);

                        diskIds.insert(msg->DiskId);
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        NPrivateProto::TRebindVolumesRequest request;
        request.SetBinding(2);   // REMOTE
        request.SetUseGentlePreemption(true);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("rebindvolumes", buf);

        UNIT_ASSERT_VALUES_EQUAL(2, diskIds.size());
        UNIT_ASSERT_VALUES_EQUAL(diskIds.count("vol0"), 1);
        UNIT_ASSERT_VALUES_EQUAL(diskIds.count("vol1"), 1);
    }

    Y_UNIT_TEST(ShouldDrainNode)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 observedNodeIdx = 0;
        NKikimrHive::EDrainDownPolicy observedDownPolicy =
            NKikimrHive::DRAIN_POLICY_NO_DOWN;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHive::EvDrainNode: {
                        auto* msg = event->Get<TEvHive::TEvDrainNode>();
                        observedNodeIdx = msg->Record.GetNodeID();
                        observedDownPolicy = msg->Record.GetDownPolicy();
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        NPrivateProto::TDrainNodeRequest request;

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("drainnode", buf);

        UNIT_ASSERT_VALUES_EQUAL(
            service.GetSender().NodeId(),
            observedNodeIdx
        );

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NKikimrHive::DRAIN_POLICY_NO_DOWN),
            static_cast<int>(observedDownPolicy));

        request.SetDownPolicy(
            NCloud::NProto::DRAIN_POLICY_KEEP_DOWN);

        buf.clear();
        google::protobuf::util::MessageToJsonString(request, &buf);

        service.ExecuteAction("drainnode", buf);

        UNIT_ASSERT_VALUES_EQUAL(
            service.GetSender().NodeId(),
            observedNodeIdx
        );

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NKikimrHive::DRAIN_POLICY_KEEP_DOWN),
            static_cast<int>(observedDownPolicy));
    }

    Y_UNIT_TEST(ShouldForwardUpdateUsedBlocksToVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvUpdateUsedBlocksRequest: {
                        auto* msg =
                            event->Get<TEvVolume::TEvUpdateUsedBlocksRequest>();
                        UNIT_ASSERT_VALUES_EQUAL(1, msg->Record.StartIndicesSize());
                        UNIT_ASSERT_VALUES_EQUAL(1, msg->Record.BlockCountsSize());
                        UNIT_ASSERT_VALUES_EQUAL(11, msg->Record.GetStartIndices(0));
                        UNIT_ASSERT_VALUES_EQUAL(22, msg->Record.GetBlockCounts(0));
                        UNIT_ASSERT_VALUES_EQUAL(true, msg->Record.GetUsed());
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "track-used";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        {
            NProto::TUpdateUsedBlocksRequest request;
            request.SetDiskId(DefaultDiskId);
            request.AddStartIndices(11);
            request.AddBlockCounts(22);
            request.SetUsed(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto executeResponse = service.ExecuteAction("updateusedblocks", buf);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, executeResponse->GetStatus());

            NProto::TUpdateUsedBlocksResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                executeResponse->Record.GetOutput(),
                &response
            ).ok());

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldRebuildMetadataForPartitionVersion1)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol0");

        auto sessionId = service.MountVolume("vol0")->Record.GetSessionId();

        service.WriteBlocks(
            "vol0",
            TBlockRange64::WithLength(0, 1024),
            sessionId,
            char(1));

        {
            NPrivateProto::TRebuildMetadataRequest request;
            request.SetDiskId("vol0");
            request.SetMetadataType(NPrivateProto::USED_BLOCKS);
            request.SetBatchSize(100);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            auto response = service.ExecuteAction("rebuildmetadata", buf);
            NPrivateProto::TRebuildMetadataResponse metadataResponse;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &metadataResponse
            ).ok());
        }

        {
            NPrivateProto::TGetRebuildMetadataStatusRequest request;
            request.SetDiskId("vol0");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            auto response = service.ExecuteAction("getrebuildmetadatastatus", buf);
            NPrivateProto::TGetRebuildMetadataStatusResponse metadataResponse;

            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &metadataResponse
            ).ok());

            UNIT_ASSERT_VALUES_EQUAL(1024, metadataResponse.GetProgress().GetTotal());
        }
    }

    Y_UNIT_TEST(ShouldRebuildMetadataBlockCountForPartitionVersion1)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume("vol0");

        auto sessionId = service.MountVolume("vol0")->Record.GetSessionId();

        service.WriteBlocks(
            "vol0",
            TBlockRange64::WithLength(0, 1024),
            sessionId,
            char(1));

        {
            NPrivateProto::TRebuildMetadataRequest request;
            request.SetDiskId("vol0");
            request.SetMetadataType(NPrivateProto::BLOCK_COUNT);
            request.SetBatchSize(100);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            auto response = service.ExecuteAction("rebuildmetadata", buf);
            NPrivateProto::TRebuildMetadataResponse metadataResponse;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &metadataResponse
            ).ok());
        }

        {
            NPrivateProto::TGetRebuildMetadataStatusRequest request;
            request.SetDiskId("vol0");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            auto response = service.ExecuteAction("getrebuildmetadatastatus", buf);
            NPrivateProto::TGetRebuildMetadataStatusResponse metadataResponse;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &metadataResponse
            ).ok());

            UNIT_ASSERT_VALUES_EQUAL(1, metadataResponse.GetProgress().GetTotal());
        }
    }

    Y_UNIT_TEST(ShouldScanDiskForPartitionVersion1)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);
        env.WriteBlob(0, 2);
        env.WriteBlob(1, 3);

        const auto result = env.ScanDiskToEnd(MakeScanDiskRequest());

        UNIT_ASSERT_LT(1, result.Records.size());
        for (ui32 i = 0; i < result.Records.size(); ++i) {
            const auto& record = result.Records[i];
            UNIT_ASSERT_VALUES_EQUAL(0, record.GetPartitionIndex());
            UNIT_ASSERT(!HasError(record.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(0, record.BlobResultsSize());
            UNIT_ASSERT_LT(0, record.GetScanCommitId());
            UNIT_ASSERT_VALUES_EQUAL(
                i + 1 == result.Records.size(),
                record.GetIsCompleted());
            if (i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    result.Records[i - 1].GetNextCursor(),
                    record.GetCursor());
            }
        }

        const auto counters = SumCounters(result.Records);
        UNIT_ASSERT_VALUES_EQUAL(2, counters.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, counters.GetDeadBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.GetBrokenBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.GetUncheckedBlobs());

        UNIT_ASSERT_VALUES_EQUAL(1, result.Status.PartitionsSize());
        const auto& partition = result.Status.GetPartitions(0);
        UNIT_ASSERT(partition.GetIsCompleted());
        UNIT_ASSERT(!HasError(partition.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            ToJson(counters),
            ToJson(partition.GetCounters()));
        UNIT_ASSERT_VALUES_EQUAL(0, result.Status.IncompletePartitionsSize());
    }

    Y_UNIT_TEST(ShouldScanEveryPartitionOfDisk)
    {
        TScanDiskBatchTestEnv env(true, 2);   // checksums, partitionCount
        for (ui32 blobIndex = 0; blobIndex < 2 * ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }

        const auto result = env.ScanDiskToEnd(MakeScanDiskRequest());

        // The partitions are scanned one after another.
        TVector<TVector<TScanDiskRecord>> partitionRecords(2);
        ui32 lastPartitionIndex = 0;
        for (const auto& record: result.Records) {
            UNIT_ASSERT(!HasError(record.GetError()));
            UNIT_ASSERT_LE(lastPartitionIndex, record.GetPartitionIndex());
            lastPartitionIndex = record.GetPartitionIndex();
            partitionRecords[lastPartitionIndex].push_back(record);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, result.Status.PartitionsSize());
        for (const auto& partition: result.Status.GetPartitions()) {
            const auto& records =
                partitionRecords[partition.GetPartitionIndex()];
            UNIT_ASSERT(!records.empty());
            UNIT_ASSERT(records.back().GetIsCompleted());
            UNIT_ASSERT(partition.GetIsCompleted());
            UNIT_ASSERT_VALUES_EQUAL(
                ScanDiskBlobsPerPartition,
                partition.GetCounters().GetCheckedBlobs());
            UNIT_ASSERT_VALUES_EQUAL(
                ToJson(SumCounters(records)),
                ToJson(partition.GetCounters()));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, result.Status.IncompletePartitionsSize());
    }

    Y_UNIT_TEST(ShouldWriteBlobsGivenUpAfterLastRetry)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        auto request = MakeScanDiskRequest();
        request.SetMaxRetries(1);
        request.SetRetryTimeoutMs(10);

        auto scanGivenUpBlob =
            [&](const NPrivateProto::TScanDiskRequest& request)
        {
            const auto result = env.ScanDiskToEnd(request);
            runtime.SetEventFilter(TTestActorRuntime::DefaultFilterFunc);

            const auto blobResults = GetBlobResults(result.Records);
            UNIT_ASSERT_VALUES_EQUAL(1, blobResults.size());
            UNIT_ASSERT_VALUES_EQUAL(
                blobIds[0],
                MakeLogoBlobId(blobResults[0].GetBlobId()));
            return std::make_pair(blobResults[0], SumCounters(result.Records));
        };

        {
            ui32 checks = 0;
            runtime.SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvBlobStorage::EvCheckIntegrity) {
                        ++checks;
                        return true;
                    }
                    return false;
                });

            const auto [result, counters] = scanGivenUpBlob(request);

            UNIT_ASSERT_VALUES_EQUAL(1 + request.GetMaxRetries(), checks);
            UNIT_ASSERT_EQUAL(
                TScanDiskBlobResult::UNCHECKED,
                result.GetVerdict());
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, result.GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetUncheckedBlobs());
            UNIT_ASSERT_VALUES_EQUAL(0, counters.GetCheckedBlobs());
        }

        {
            runtime.SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() == TEvBlobStorage::EvGetResult)
                    {
                        auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                        if (msg->Responses[0].Id == blobIds[0]) {
                            msg->Status = NKikimrProto::ERROR;
                            msg->ErrorReason = "crafted error";
                        }
                    }
                    return false;
                });

            auto checksumRequest = request;
            checksumRequest.SetVerifyChecksums(true);
            const auto [result, counters] = scanGivenUpBlob(checksumRequest);

            UNIT_ASSERT_EQUAL(
                TScanDiskBlobResult::READ_FAILED,
                result.GetVerdict());
            UNIT_ASSERT_STRING_CONTAINS(
                result.GetError().GetMessage(),
                "crafted error");
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetReadFailedBlobs());
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetCheckedBlobs());
        }

        {
            // The blob comes back lost, and the partition fails every
            // confirmation.
            runtime.SetEventFilter(
                [&](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvVolume::EvScanDiskBatchRequest)
                    {
                        using TEv = TEvVolume::TEvScanDiskBatchRequest;
                        auto& record = event->Get<TEv>()->Record;
                        if (record.BlobsToConfirmSize()) {
                            record.SetCursor("not a cursor");
                        }
                    }
                    return ReplyThatBlobIsLost(runtime, event);
                });

            const auto [result, counters] = scanGivenUpBlob(request);

            UNIT_ASSERT_EQUAL(TScanDiskBlobResult::BROKEN, result.GetVerdict());
            UNIT_ASSERT(result.GetUnconfirmed());
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetUnconfirmedBlobs());
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetBrokenBlobs());
            UNIT_ASSERT_VALUES_EQUAL(1, counters.GetCheckedBlobs());
        }
    }

    Y_UNIT_TEST(ShouldGiveSameTotalsWhenStartedFromRecord)
    {
        TScanDiskBatchTestEnv env;
        for (ui32 blobIndex = 0; blobIndex < ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }
        env.WriteBlob(0, 2);

        const auto full = env.ScanDiskToEnd(MakeScanDiskRequest());
        UNIT_ASSERT_LT(2, full.Records.size());

        const auto& resumeRecord = full.Records[1];
        auto request = MakeScanDiskRequest();
        request.SetCursor(resumeRecord.GetNextCursor());
        request.SetMaxCommitId(resumeRecord.GetScanCommitId());
        const auto rest = env.ScanDiskToEnd(request);

        TVector<TScanDiskRecord> records(
            full.Records.begin(),
            full.Records.begin() + 2);
        records.insert(records.end(), rest.Records.begin(), rest.Records.end());

        const auto counters = SumCounters(records);
        UNIT_ASSERT_VALUES_EQUAL(
            ToJson(SumCounters(full.Records)),
            ToJson(counters));
        UNIT_ASSERT_VALUES_EQUAL(
            ScanDiskBlobsPerPartition,
            counters.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, counters.GetDeadBlobs());
        for (const auto& record: rest.Records) {
            UNIT_ASSERT_VALUES_EQUAL(
                resumeRecord.GetScanCommitId(),
                record.GetScanCommitId());
        }
    }

    Y_UNIT_TEST(ShouldReportEveryBlobOnceWithAdjacentCommitIdRanges)
    {
        TScanDiskBatchTestEnv env;
        for (ui32 blobIndex = 0; blobIndex < ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }
        env.GetRuntime().SetEventFilter(ReplyThatBlobIsLost);

        const auto allBlobIds = GetReportedBlobIds(
            env.ScanDiskToEnd(MakeScanDiskRequest()).Records);
        UNIT_ASSERT_VALUES_EQUAL(ScanDiskBlobsPerPartition, allBlobIds.size());

        const ui64 lastCommitIdOfFirstRange = GetCommitId(allBlobIds[1]);

        auto firstRange = MakeScanDiskRequest();
        firstRange.SetMaxCommitId(lastCommitIdOfFirstRange);
        auto blobIds =
            GetReportedBlobIds(env.ScanDiskToEnd(firstRange).Records);
        UNIT_ASSERT_VALUES_EQUAL(2, blobIds.size());

        auto secondRange = MakeScanDiskRequest();
        secondRange.SetMinCommitId(lastCommitIdOfFirstRange + 1);
        const auto secondBlobIds =
            GetReportedBlobIds(env.ScanDiskToEnd(secondRange).Records);
        UNIT_ASSERT_VALUES_EQUAL(2, secondBlobIds.size());

        blobIds.insert(
            blobIds.end(),
            secondBlobIds.begin(),
            secondBlobIds.end());
        Sort(blobIds);
        UNIT_ASSERT_VALUES_EQUAL(allBlobIds, blobIds);
    }

    Y_UNIT_TEST(ShouldRejectInvalidScanDiskInput)
    {
        TScanDiskBatchTestEnv env(true, 2);   // checksums, partitionCount
        const TString singlePartitionDiskId = "vol1";
        env.Service->CreateVolume(
            singlePartitionDiskId,
            ScanDiskBlobsPerPartition * ScanDiskBlobBlockCount);

        auto check = [&](const TString& input)
        {
            const auto response = env.SendScanDisk(input);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                input + ": " + response->GetErrorReason());
        };

        auto checkRequest =
            [&](std::function<void(NPrivateProto::TScanDiskRequest&)> change)
        {
            auto request = MakeScanDiskRequest();
            change(request);
            check(ToJson(request));
        };

        check("not json");
        check(R"({"DiskId": "vol0", "UnknownField": 1})");
        checkRequest([](auto& request) { request.ClearDiskId(); });

        checkRequest(
            [](auto& request)
            {
                request.SetPartitionIndex(0);
                request.SetMinCommitId(10);
                request.SetMaxCommitId(9);
            });
        checkRequest(
            [](auto& request)
            {
                request.SetPartitionIndex(0);
                request.SetMinCommitId(10);
                request.SetCursor("cursor");
            });
        checkRequest([](auto& request) { request.SetPartitionIndex(2); });
        checkRequest(
            [&](auto& request)
            {
                request.SetDiskId(singlePartitionDiskId);
                request.SetPartitionIndex(1);
            });

        // Commit ids and cursors belong to one partition.
        checkRequest([](auto& request) { request.SetMinCommitId(10); });
        checkRequest([](auto& request) { request.SetMaxCommitId(10); });
        checkRequest([](auto& request) { request.SetCursor("cursor"); });

        // A single-partition disk takes them without PartitionIndex.
        auto request = MakeScanDiskRequest();
        request.SetDiskId(singlePartitionDiskId);
        request.SetMaxCommitId(10);
        env.PollScanDiskToEnd(env.StartScanDisk(request));
    }

    Y_UNIT_TEST(ShouldLeavePartitionIncompleteAfterUnavailableTimeout)
    {
        TScanDiskBatchTestEnv env(true, 2);   // checksums, partitionCount
        for (ui32 blobIndex = 0; blobIndex < 2 * ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }

        ui32 rejectedRequests = 0;
        env.GetRuntime().SetEventFilter(
            MakeRejectPartitionFilter(0, rejectedRequests));

        auto request = MakeScanDiskRequest();
        request.SetRetryTimeoutMs(100);
        request.SetUnavailableTimeoutMs(5000);
        const auto result = env.ScanDiskToEnd(request);

        UNIT_ASSERT_LT(1, rejectedRequests);

        const auto& failedRecord = result.Records.front();
        UNIT_ASSERT_VALUES_EQUAL(0, failedRecord.GetPartitionIndex());
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, failedRecord.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL("", failedRecord.GetCursor());
        UNIT_ASSERT(!failedRecord.GetIsCompleted());

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>{0},
            TVector<ui32>(
                result.Status.GetIncompletePartitions().begin(),
                result.Status.GetIncompletePartitions().end()));

        const auto& failedPartition = result.Status.GetPartitions(0);
        UNIT_ASSERT(!failedPartition.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            failedPartition.GetError().GetCode());

        const auto& nextPartition = result.Status.GetPartitions(1);
        UNIT_ASSERT(nextPartition.GetIsCompleted());
        UNIT_ASSERT(!HasError(nextPartition.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            ScanDiskBlobsPerPartition,
            nextPartition.GetCounters().GetCheckedBlobs());
    }

    Y_UNIT_TEST(ShouldLoseNothingWhenPartitionRebootsMidScan)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        for (ui32 blobIndex = 0; blobIndex < ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }
        const ui64 partitionTabletId = env.GetPartitionTabletId();

        // Holds the second listing, which is the first with a cursor.
        TAutoPtr<IEventHandle> heldListing;
        bool listingHeld = false;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (listingHeld || event->GetTypeRewrite() !=
                                       TEvVolume::EvScanDiskBatchRequest)
                {
                    return false;
                }

                using TEv = TEvVolume::TEvScanDiskBatchRequest;
                const auto& record = event->Get<TEv>()->Record;
                if (record.GetCount() && record.GetCursor()) {
                    heldListing = event.Release();
                    listingHeld = true;
                    return true;
                }
                return false;
            });

        const auto operationId = env.StartScanDisk(MakeScanDiskRequest());
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return listingHeld;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT(listingHeld);

        RebootTablet(
            runtime,
            partitionTabletId,
            env.Service->GetSender(),
            env.NodeIdx);
        runtime.Send(heldListing.Release(), env.NodeIdx);

        const auto result = env.PollScanDiskToEnd(operationId);

        for (const auto& record: result.Records) {
            UNIT_ASSERT(!HasError(record.GetError()));
        }
        const auto counters = SumCounters(result.Records);
        UNIT_ASSERT_VALUES_EQUAL(
            ScanDiskBlobsPerPartition,
            counters.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.GetUncheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, result.Status.IncompletePartitionsSize());
    }

    Y_UNIT_TEST(ShouldReturnRecordsUntilAcknowledged)
    {
        TScanDiskBatchTestEnv env;
        for (ui32 blobIndex = 0; blobIndex < ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }
        env.GetRuntime().SetEventFilter(ReplyThatBlobIsLost);

        const auto operationId = env.StartScanDisk(MakeScanDiskRequest());

        TScanDiskStatus status;
        for (ui32 poll = 0; !status.GetIsCompleted(); ++poll) {
            UNIT_ASSERT_LT(poll, 100);
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
            status = env.GetScanDiskStatus(operationId, 0);
        }
        UNIT_ASSERT_LE(ScanDiskBlobsPerPartition, status.RecordsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, status.GetRecords(0).GetRecordId());

        // A lost reply loses nothing.
        UNIT_ASSERT_VALUES_EQUAL(
            ToJson(status),
            ToJson(env.GetScanDiskStatus(operationId, 0)));

        const ui64 ackedRecordId = status.GetRecords(1).GetRecordId();
        const auto rest = env.GetScanDiskStatus(operationId, ackedRecordId);
        UNIT_ASSERT_VALUES_EQUAL(status.RecordsSize() - 2, rest.RecordsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            ackedRecordId + 1,
            rest.GetRecords(0).GetRecordId());

        // An older acknowledgement brings nothing back.
        UNIT_ASSERT_VALUES_EQUAL(
            ToJson(rest),
            ToJson(env.GetScanDiskStatus(operationId, 0)));

        env.PollScanDiskToEnd(operationId, ackedRecordId);
    }

    Y_UNIT_TEST(ShouldPauseListingAtMaxPendingBlobResults)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        for (ui32 blobIndex = 0; blobIndex < ScanDiskBlobsPerPartition;
             ++blobIndex)
        {
            env.WriteBlob(blobIndex, 1);
        }
        runtime.SetEventFilter(ReplyThatBlobIsLost);

        auto request = MakeScanDiskRequest();
        request.SetMaxPendingBlobResults(2);
        const auto operationId = env.StartScanDisk(request);

        auto getRecordIds = [&](ui64 ackedRecordId)
        {
            runtime.DispatchEvents({}, TDuration::Seconds(2));
            const auto status =
                env.GetScanDiskStatus(operationId, ackedRecordId);
            UNIT_ASSERT(!status.GetIsCompleted());

            TVector<ui64> recordIds;
            for (const auto& record: status.GetRecords()) {
                UNIT_ASSERT_VALUES_EQUAL(1, record.BlobResultsSize());
                recordIds.push_back(record.GetRecordId());
            }
            return recordIds;
        };

        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{1, 2}), getRecordIds(0));
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{1, 2}), getRecordIds(0));

        // The acknowledgement makes room for one more batch.
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{2}), getRecordIds(1));
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{2, 3}), getRecordIds(1));

        const auto result = env.PollScanDiskToEnd(operationId, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, result.Records.front().GetRecordId());
    }

    Y_UNIT_TEST(ShouldStopScanDiskWhenNobodyPolls)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);
        // The scan doesn't finish: no check ever replies.
        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                return event->GetTypeRewrite() ==
                       TEvBlobStorage::EvCheckIntegrity;
            });

        auto request = MakeScanDiskRequest();
        request.SetPollTimeoutMs(2000);
        const auto operationId = env.StartScanDisk(request);

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(1500));
        UNIT_ASSERT(!env.GetScanDiskStatus(operationId, 0).GetIsCompleted());

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(1500));
        UNIT_ASSERT(!env.GetScanDiskStatus(operationId, 0).GetIsCompleted());

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(3));
        env.CheckScanDiskNotFound(operationId);
    }

    Y_UNIT_TEST(ShouldNotFindUnknownScanDisk)
    {
        TScanDiskBatchTestEnv env;
        env.CheckScanDiskNotFound(CreateGuidAsString());

        env.Service->SendExecuteActionRequest("getscandiskstatus", "{}");
        const auto response = env.Service->RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldScanDiskBatch)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);
        env.WriteBlob(0, 2);
        env.WriteBlob(1, 3);

        const auto response = env.ScanDiskBatch(MakeScanDiskBatchRequest());

        UNIT_ASSERT(response.GetIsCompleted());
        UNIT_ASSERT(response.GetNextCursor());
        UNIT_ASSERT_LT(0, response.GetScanCommitId());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetDeadBlobs());
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetBrokenBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetDeletedDuringScan());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobResultsSize());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
    }

    Y_UNIT_TEST(ShouldReportLostBlobStillInBlobIndex)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);
        env.WriteBlob(1, 2);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(2, blobIds.size());

        THashMap<TLogoBlobID, TCheckIntegrityResult::EPlacementStatus>
            placementStatuses = {
                {blobIds[0], TCheckIntegrityResult::PS_BLOB_IS_LOST}};
        env.GetRuntime().SetEventFilter(
            MakeCheckIntegrityFilter(placementStatuses));

        const auto response = env.ScanDiskBatch(MakeScanDiskBatchRequest());

        UNIT_ASSERT_VALUES_EQUAL(2, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetBrokenBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobResultsSize());

        const auto& result = response.GetBlobResults(0);
        UNIT_ASSERT_VALUES_EQUAL(
            blobIds[0],
            MakeLogoBlobId(result.GetBlobId()));
        UNIT_ASSERT_EQUAL(TScanDiskBlobResult::BROKEN, result.GetVerdict());
        UNIT_ASSERT_VALUES_EQUAL("OK", result.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(
            "PS_BLOB_IS_LOST",
            result.GetPlacementStatus());
        UNIT_ASSERT_VALUES_EQUAL("crafted", result.GetDataInfo());
        UNIT_ASSERT(!result.GetUnconfirmed());
    }

    Y_UNIT_TEST(ShouldDropBlobDeletedBetweenListingAndCheck)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);

        TVector<TAutoPtr<IEventHandle>> heldChecks;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrity)
                {
                    heldChecks.push_back(event.Release());
                    return true;
                }
                return false;
            });

        const auto owner = env.SendScanDiskBatch(MakeScanDiskBatchRequest());
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return !heldChecks.empty();
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, heldChecks.size());
        const auto blobId =
            heldChecks[0]->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id;

        // A write makes the blob dead, and compaction and cleanup delete it
        // from the blob index.
        env.WriteBlob(0, 2);
        NPrivateProto::TCompactRangeRequest compactRequest;
        compactRequest.SetDiskId(DefaultDiskId);
        compactRequest.SetBlocksCount(ScanDiskBlobBlockCount);
        env.Service->ExecuteAction("compactrange", ToJson(compactRequest));

        NPrivateProto::TDescribeBlobRequest describeRequest;
        describeRequest.SetDiskId(DefaultDiskId);
        auto& describeBlobId = *describeRequest.MutableBlobId();
        describeBlobId.SetRawX1(blobId.GetRaw()[0]);
        describeBlobId.SetRawX2(blobId.GetRaw()[1]);
        describeBlobId.SetRawX3(blobId.GetRaw()[2]);
        for (ui32 attempt = 0;; ++attempt) {
            UNIT_ASSERT_LT(attempt, 100);
            const auto response = env.Service->ExecuteAction(
                "describeblob",
                ToJson(describeRequest));
            NPrivateProto::TDescribeBlobResponse describe;
            google::protobuf::util::JsonParseOptions options;
            options.ignore_unknown_fields = true;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                            response->Record.GetOutput(),
                            &describe,
                            options)
                            .ok());
            if (!describe.BlocksSize()) {
                break;
            }
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        }

        // The disks have started dropping the blob.
        runtime.SetEventFilter(TTestActorRuntime::DefaultFilterFunc);
        ReplyToCheckIntegrity(
            runtime,
            *heldChecks[0],
            TCheckIntegrityResult::PS_BLOB_IS_LOST);

        const auto response = ParseScanDiskBatchResponse(
            *env.RecvScanDiskBatchResponse(owner)->Get());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetDeletedDuringScan());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetBrokenBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobResultsSize());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
    }

    Y_UNIT_TEST(ShouldRetryBlobWithoutCheckReplyByDeadline)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                return event->GetTypeRewrite() ==
                       TEvBlobStorage::EvCheckIntegrity;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetDeadlineMs(1000);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT(response.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobResultsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobsToRetrySize());

        const auto& blobToRetry = response.GetBlobsToRetry(0);
        UNIT_ASSERT(blobToRetry.GetHasChecksums());
        UNIT_ASSERT_LT(0, blobToRetry.GetBSGroupId());
        UNIT_ASSERT_EQUAL(
            TScanDiskBlobResult::UNCHECKED,
            blobToRetry.GetResult().GetVerdict());
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            blobToRetry.GetResult().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldRecheckBlobsWithoutListing)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                return event->GetTypeRewrite() ==
                       TEvBlobStorage::EvCheckIntegrity;
            });

        auto listingRequest = MakeScanDiskBatchRequest();
        listingRequest.SetDeadlineMs(1000);
        const auto listingResponse = env.ScanDiskBatch(listingRequest);
        UNIT_ASSERT_VALUES_EQUAL(1, listingResponse.BlobsToRetrySize());

        ui32 listingRequests = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvScanDiskBatchRequest) {
                    using TEv = TEvVolume::TEvScanDiskBatchRequest;
                    listingRequests += event->Get<TEv>()->Record.GetCount() > 0;
                }
                return false;
            });

        // The client sends BlobsToRetry back as they came.
        auto request = MakeScanDiskBatchRequest(0);
        *request.MutableBlobsToRecheck() = listingResponse.GetBlobsToRetry();
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(0, listingRequests);
        UNIT_ASSERT(!response.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetDeadBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
    }

    Y_UNIT_TEST(ShouldReportChecksumMismatch)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        // The same corruption in every part: only the checksums show it.
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    auto& response = msg->Responses[0];
                    if (response.Id == blobIds[0] && response.Shift == 0) {
                        auto data = response.Buffer.ConvertToString();
                        data.begin()[0] ^= 1;
                        response.Buffer = TRope(data);
                    }
                }
                return false;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(1, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetChecksumMismatchBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetChecksumVerifiedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobResultsSize());

        const auto& result = response.GetBlobResults(0);
        UNIT_ASSERT_EQUAL(
            TScanDiskBlobResult::CHECKSUM_MISMATCH,
            result.GetVerdict());
        UNIT_ASSERT_VALUES_EQUAL(1, result.GetMismatchedBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1, result.MismatchesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, result.GetMismatches(0).GetBlockIndex());
        UNIT_ASSERT_VALUES_EQUAL(0, result.GetMismatches(0).GetBlobOffset());
        // The integrity check found the parts consistent.
        UNIT_ASSERT_VALUES_EQUAL("PS_OK", result.GetPlacementStatus());
    }

    Y_UNIT_TEST(ShouldKeepDegradedVerdictWhenChecksumsMatch)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        THashMap<TLogoBlobID, TCheckIntegrityResult::EPlacementStatus>
            placementStatuses = {
                {blobIds[0], TCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE}};
        env.GetRuntime().SetEventFilter(
            MakeCheckIntegrityFilter(placementStatuses));

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(1, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetDegradedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetChecksumVerifiedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobResultsSize());
        UNIT_ASSERT_EQUAL(
            TScanDiskBlobResult::DEGRADED,
            response.GetBlobResults(0).GetVerdict());
    }

    Y_UNIT_TEST(ShouldVerifyChecksumsOnlyOfReadableBlobs)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);
        env.WriteBlob(1, 2);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(2, blobIds.size());

        THashMap<TLogoBlobID, TCheckIntegrityResult::EPlacementStatus>
            placementStatuses = {
                {blobIds[0], TCheckIntegrityResult::PS_BLOB_IS_LOST}};
        auto checkIntegrityFilter = MakeCheckIntegrityFilter(placementStatuses);

        TVector<TLogoBlobID> verifiedBlobIds;
        ui32 verifyRequests = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvVerifyBlobChecksumsRequest)
                {
                    using TEv = TEvVolume::TEvVerifyBlobChecksumsRequest;
                    ++verifyRequests;
                    for (const auto& blobId:
                         event->Get<TEv>()->Record.GetBlobIds()) {
                        verifiedBlobIds.push_back(
                            LogoBlobIDFromLogoBlobID(blobId));
                    }
                }
                return checkIntegrityFilter(runtime, event);
            });

        env.ScanDiskBatch(MakeScanDiskBatchRequest());
        UNIT_ASSERT_VALUES_EQUAL(0, verifyRequests);

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_LT(0, verifyRequests);
        SortUnique(verifiedBlobIds);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TLogoBlobID>{blobIds[1]},
            verifiedBlobIds);
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetBrokenBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetChecksumVerifiedBlobs());
    }

    Y_UNIT_TEST(ShouldNotVerifyBlobsWithoutChecksums)
    {
        TScanDiskBatchTestEnv env(false);   // checksums
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);

        ui32 verifyRequests = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                verifyRequests += event->GetTypeRewrite() ==
                                  TEvVolume::EvVerifyBlobChecksumsRequest;
                return false;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(0, verifyRequests);
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetChecksumVerifiedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobsToRetrySize());
    }

    Y_UNIT_TEST(ShouldReportFailedChecksumReadAsReadFailed)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    if (msg->Responses[0].Id == blobIds[0]) {
                        msg->Status = NKikimrProto::ERROR;
                        msg->ErrorReason = "crafted error";
                    }
                }
                return false;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(0, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobResultsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobsToRetrySize());

        const auto& result = response.GetBlobsToRetry(0).GetResult();
        UNIT_ASSERT_EQUAL(
            TScanDiskBlobResult::READ_FAILED,
            result.GetVerdict());
        UNIT_ASSERT(!result.GetUnconfirmed());
        UNIT_ASSERT_STRING_CONTAINS(
            result.GetError().GetMessage(),
            "crafted error");
    }

    Y_UNIT_TEST(ShouldNotVerifyChecksumsWhenChecksumRequestFails)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvVerifyBlobChecksumsResponse)
                {
                    using TEv = TEvVolume::TEvVerifyBlobChecksumsResponse;
                    auto& record = event->Get<TEv>()->Record;
                    record.Clear();
                    *record.MutableError() =
                        MakeError(E_REJECTED, "crafted error");
                }
                return false;
            });

        auto request = MakeScanDiskBatchRequest();
        request.SetVerifyChecksums(true);
        const auto response = env.ScanDiskBatch(request);

        UNIT_ASSERT_VALUES_EQUAL(0, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobsToRetrySize());

        const auto& result = response.GetBlobsToRetry(0).GetResult();
        UNIT_ASSERT_EQUAL(TScanDiskBlobResult::HEALTHY, result.GetVerdict());
        UNIT_ASSERT(result.GetChecksumsNotVerified());
        UNIT_ASSERT(!result.GetUnconfirmed());
    }

    Y_UNIT_TEST(ShouldFlagVerdictsUnconfirmedWhenConfirmationFails)
    {
        TScanDiskBatchTestEnv env;
        auto& runtime = env.GetRuntime();
        env.WriteBlob(0, 1);
        const auto blobIds = env.ListBlobs();
        UNIT_ASSERT_VALUES_EQUAL(1, blobIds.size());

        THashMap<TLogoBlobID, TCheckIntegrityResult::EPlacementStatus>
            placementStatuses = {
                {blobIds[0], TCheckIntegrityResult::PS_BLOB_IS_LOST}};
        auto checkIntegrityFilter = MakeCheckIntegrityFilter(placementStatuses);

        // The partition fails the confirmation request.
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvScanDiskBatchRequest) {
                    using TEv = TEvVolume::TEvScanDiskBatchRequest;
                    auto& record = event->Get<TEv>()->Record;
                    if (record.BlobsToConfirmSize()) {
                        record.SetCursor("not a cursor");
                    }
                }
                return checkIntegrityFilter(runtime, event);
            });

        const auto response = env.ScanDiskBatch(MakeScanDiskBatchRequest());

        UNIT_ASSERT_VALUES_EQUAL(0, response.GetCheckedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(0, response.BlobResultsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.BlobsToRetrySize());

        const auto& result = response.GetBlobsToRetry(0).GetResult();
        UNIT_ASSERT_EQUAL(TScanDiskBlobResult::BROKEN, result.GetVerdict());
        UNIT_ASSERT(result.GetUnconfirmed());
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldFailScanDiskBatchWhenListingFails)
    {
        TScanDiskBatchTestEnv env;
        env.WriteBlob(0, 1);

        auto check = [&](const NPrivateProto::TScanDiskBatchRequest& request)
        {
            const auto response =
                env.RecvScanDiskBatchResponse(env.SendScanDiskBatch(request));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->Get()->GetStatus(),
                response->Get()->GetErrorReason());
        };

        {
            auto request = MakeScanDiskBatchRequest();
            request.SetCursor("not a cursor");
            check(request);
        }

        {
            auto request = MakeScanDiskBatchRequest();
            request.SetPartitionIndex(1);
            check(request);
        }

        {
            auto request = MakeScanDiskBatchRequest();
            request.ClearDiskId();
            check(request);
        }
    }

    Y_UNIT_TEST(ShouldForceMigrate)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        auto* disk = drState->Disks.FindPtr(DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(800, disk->Devices.size());

        NProto::TStartForceMigrationRequest request;
        request.SetSourceDiskId(DefaultDiskId);
        request.SetSourceDeviceId(disk->Devices[0].GetDeviceUUID());
        request.SetTargetDeviceId("uuid801");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        const auto response = service.ExecuteAction("migrationdiskregistrydevice", buf);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCreateDiskFromDevices)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        NProto::TCreateVolumeFromDevicesRequest request;
        request.SetDiskId("Disk-1");
        request.SetBlockSize(512);
        auto* devices = request.MutableDeviceUUIDs();
        devices->Add("uuid1");
        devices->Add("uuid42");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        const auto response = service.ExecuteAction("creatediskfromdevices", buf);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCreateDiskFromDevicesWithStorageMediaKind)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        struct TTestCase
        {
            NCloud::NProto::EStorageMediaKind Requested;
            NCloud::NProto::EStorageMediaKind Expected;
        };

        const TVector<TTestCase> testCases = {
            {NCloud::NProto::STORAGE_MEDIA_DEFAULT,
             NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL},
            {NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
             NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED},
            {NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED,
             NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED},
            {NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL,
             NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL},
            {NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL,
             NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL},
        };

        for (size_t i = 0; i != testCases.size(); ++i) {
            const auto& testCase = testCases[i];
            const TString diskId = Sprintf("Disk-%zu", i + 1);

            NProto::TCreateVolumeFromDevicesRequest request;
            request.SetDiskId(diskId);
            request.SetBlockSize(DefaultBlockSize);
            request.SetStorageMediaKind(testCase.Requested);
            request.MutableDeviceUUIDs()->Add(Sprintf("uuid%zu", i + 1));

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            service.ExecuteAction("creatediskfromdevices", buf);

            const auto response = service.DescribeVolume(diskId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                static_cast<int>(testCase.Expected),
                static_cast<int>(
                    response->Record.GetVolume().GetStorageMediaKind()),
                diskId);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMediaKindsForDiskFromDevices)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        ui32 nodeIdx = SetupTestEnv(env);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        for (auto mediaKind:
             {NCloud::NProto::STORAGE_MEDIA_SSD,
              NCloud::NProto::STORAGE_MEDIA_HYBRID,
              NCloud::NProto::STORAGE_MEDIA_HDD,
              NCloud::NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP})
        {
            NProto::TCreateVolumeFromDevicesRequest request;
            request.SetDiskId("Disk-1");
            request.SetBlockSize(DefaultBlockSize);
            request.SetStorageMediaKind(mediaKind);
            request.MutableDeviceUUIDs()->Add("uuid1");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            service.SendExecuteActionRequest("creatediskfromdevices", buf);
            const auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                MediaKindToString(mediaKind));
        }

        UNIT_ASSERT(drState->Disks.empty());
    }

    Y_UNIT_TEST(ShouldChangeDiskDevice)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            "Disk-1",
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED
        );

        NProto::TChangeDiskDeviceRequest request;
        request.SetDiskId("Disk-1");
        request.SetSourceDeviceId("uuid1");
        request.SetTargetDeviceId("uuid1000");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        const auto response = service.ExecuteAction("changediskdevice", buf);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldSetupChannels)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume("vol0");
        {
            NPrivateProto::TSetupChannelsRequest request;
            request.SetDiskId("vol0");
            request.MutableVolumeChannelsToPoolsKinds()->MutableData()->insert(
                {0, "pool-kind-2"});
            request.MutableVolumeChannelsToPoolsKinds()->MutableData()->insert(
                {1, "pool-kind-2"});
            request.SetIsPartitionsPoolKindSetManually(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            const auto response = service.ExecuteAction("setupchannels", buf);
            NPrivateProto::TSetupChannelsResponse setupChannelsResponse;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &setupChannelsResponse
            ).ok());
        }

        auto getPoolKindFromConfig = [](
            const NKikimrBlockStore::TVolumeConfig& volumeConfig,
            const ui32 ind)
        {
            return volumeConfig.GetExplicitChannelProfiles(ind).GetPoolKind();
        };

        bool describeReceived = false;
        ui32 requestCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeResponse: {
                        describeReceived = true;
                        auto* msg = event->Get<TEvSSProxy::TEvDescribeVolumeResponse>();
                        const auto& pathDescription = msg->PathDescription;
                        const auto& volumeDescription =
                            pathDescription.GetBlockStoreVolumeDescription();
                        const auto& volumeConfig = volumeDescription.GetVolumeConfig();
                        if (requestCount > 3) {
                            auto pool_kind = getPoolKindFromConfig(volumeConfig, 0);
                            UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-1");
                            pool_kind = getPoolKindFromConfig(volumeConfig, 1);
                            UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-1");
                            pool_kind = getPoolKindFromConfig(volumeConfig, 2);
                            UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-1");
                            break;
                        }
                        auto pool_kind = getPoolKindFromConfig(volumeConfig, 0);
                        UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-2");
                        pool_kind = getPoolKindFromConfig(volumeConfig, 1);
                        UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-2");
                        if (requestCount > 1) {
                            pool_kind = getPoolKindFromConfig(volumeConfig, 2);
                            UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-2");
                        }
                        if (requestCount > 2) {
                            pool_kind = getPoolKindFromConfig(volumeConfig, 3);
                            UNIT_ASSERT_EQUAL(pool_kind, "pool-kind-2");
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        requestCount += 1;
        service.DescribeVolume("vol0");
        UNIT_ASSERT(describeReceived);

        // First two channels shouldn't change after another request
        {
            NPrivateProto::TSetupChannelsRequest request;
            request.SetDiskId("vol0");
            request.MutableVolumeChannelsToPoolsKinds()->MutableData()->insert(
                {2, "pool-kind-2"});
            request.SetIsPartitionsPoolKindSetManually(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            const auto response = service.ExecuteAction("setupchannels", buf);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Check that nothing changes after resizing
        {
            auto request = service.CreateResizeVolumeRequest(
                "vol0",
                42949672
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        requestCount += 1;
        describeReceived = false;
        service.DescribeVolume("vol0");
        UNIT_ASSERT(describeReceived);

        // Channels shouldn't change after setting false flag
        {
            NPrivateProto::TSetupChannelsRequest request;
            request.SetDiskId("vol0");
            request.SetIsPartitionsPoolKindSetManually(false);
            request.MutableVolumeChannelsToPoolsKinds()->MutableData()->insert(
                {3, "pool-kind-2"});

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            const auto response = service.ExecuteAction("setupchannels", buf);
        }

        requestCount += 1;
        describeReceived = false;
        service.DescribeVolume("vol0");
        UNIT_ASSERT(describeReceived);

        // All channels should be from config after resizeVolume
        {
            auto request = service.CreateResizeVolumeRequest(
                "vol0",
                42949680
            );
            service.SendRequest(MakeStorageServiceId(), std::move(request));

            auto response = service.RecvResizeVolumeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        requestCount += 1;
        describeReceived = false;
        service.DescribeVolume("vol0");
        UNIT_ASSERT(describeReceived);
    }

    void UpdateDiskRegistryAgentListParamsTest(
        ui32 returnedCode,
        ui32 expectedErrorFlags)
    {
        TTestEnv env(1, 1, 4, 1, MakeIntrusive<TDiskRegistryState>());

        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        NProto::TDiskRegistryAgentListRequestParams requestParams;
        requestParams.AddAgentIds("agent0");
        requestParams.SetTimeoutMs(600);
        requestParams.SetNewNonReplicatedAgentMinTimeoutMs(123);
        requestParams.SetNewNonReplicatedAgentMaxTimeoutMs(456);

        bool updateParamsReceived = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvUpdateDiskRegistryAgentListParamsRequest: {
                        updateParamsReceived = true;
                        auto* msg = event->Get<TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest>();
                        const auto& params = msg->Record.GetParams();
                        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(requestParams, params));
                        break;
                    }
                    case TEvDiskRegistry::EvUpdateDiskRegistryAgentListParamsResponse: {
                        auto* msg = event->Get<TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse>();
                        msg->Record.MutableError()->SetCode(returnedCode);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TString buf;
        google::protobuf::util::MessageToJsonString(requestParams, &buf);
        service.SendExecuteActionRequest("updatediskregistryagentlistparams", buf);
        const auto response = service.RecvExecuteActionResponse();

        UNIT_ASSERT(updateParamsReceived);
        UNIT_ASSERT_VALUES_EQUAL(returnedCode, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(expectedErrorFlags, response->GetError().GetFlags());
    }

    Y_UNIT_TEST(ShouldForwardUpdateParamsRequestsToDiskRegistry) {
        UpdateDiskRegistryAgentListParamsTest(S_OK, NProto::EF_NONE);
    }

    Y_UNIT_TEST(ShouldForwardUpdateParamsRequestsToDiskRegistryAndSilentNotFound) {
        UpdateDiskRegistryAgentListParamsTest(E_NOT_FOUND, NProto::EF_SILENT);
    }

    Y_UNIT_TEST(ShouldFinishFillDisk)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto request = service.CreateCreateVolumeRequest();
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));

        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetStatus());

        auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(false, volumeConfig.GetIsFillFinished());

        {
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetFillGeneration(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("finishfilldisk", buf);
        }

        volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(true, volumeConfig.GetIsFillFinished());
    }

    Y_UNIT_TEST(ShouldValidateFinishFillDisk)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto request = service.CreateCreateVolumeRequest();
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));

        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetStatus());

        {
            // Incorrect ConfigVersion.
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(2);
            request.SetFillGeneration(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("FinishFillDisk", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->GetStatus());
        }

        {
            // ConfigVersion should be supplied.
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetFillGeneration(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("FinishFillDisk", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            // FillGeneration should be supplied.
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("FinishFillDisk", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            // Incorrect FillGeneration.
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetFillGeneration(2);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("FinishFillDisk", buf);
            auto response = service.RecvExecuteActionResponse();

            ui32 errCode = MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPreconditionFailed);
            UNIT_ASSERT_VALUES_EQUAL(errCode, response->GetStatus());
        }

        auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(false, volumeConfig.GetIsFillFinished());
    }

    Y_UNIT_TEST(ShouldUpdateVolumeParams)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

        TVolumeParamMap volumeParams;
        NProto::TUpdateVolumeParamsMapValue protoParam;
        protoParam.SetValue("10s");
        protoParam.SetTtlMs(100'000'000);
        volumeParams.insert({"max-timed-out-device-state-duration", protoParam});

        bool requestReceived = false;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvVolume::EvUpdateVolumeParamsRequest) {
                    auto* msg = event->Get<TEvVolume::TEvUpdateVolumeParamsRequest>();

                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetDiskId(), DefaultDiskId);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetVolumeParams(), volumeParams);
                    requestReceived = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        NProto::TUpdateVolumeParamsRequest request;
        request.SetDiskId(DefaultDiskId);
        *request.MutableVolumeParams() = volumeParams;

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        service.ExecuteAction("UpdateVolumeParams", buf);

        UNIT_ASSERT(requestReceived);

        service.DestroyVolume();
    }

    void ShouldGetDependentDevicesTest(
        TVector<TString> returnedDiskIds,
        ui32 returnedCode,
        ui32 expectedErrorFlags)
    {
        TTestEnv env(1, 1, 4, 1, MakeIntrusive<TDiskRegistryState>());

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        bool requestReceived = false;

        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvGetDependentDisksRequest: {
                        auto* msg = event->Get<TEvDiskRegistry::TEvGetDependentDisksRequest>();

                        UNIT_ASSERT_VALUES_EQUAL("localhost", msg->Record.GetHost());

                        requestReceived = true;
                        break;
                    }
                    case TEvDiskRegistry::EvGetDependentDisksResponse: {
                        auto* msg = event->Get<TEvDiskRegistry::TEvGetDependentDisksResponse>();
                        msg->Record.MutableDependentDiskIds()->Add(
                            returnedDiskIds.begin(),
                            returnedDiskIds.end());
                        msg->Record.MutableError()->SetCode(returnedCode);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        NProto::TGetDependentDisksRequest request;
        request.SetHost("localhost");
        TString jsonInput;
        google::protobuf::util::MessageToJsonString(request, &jsonInput);

        service.SendExecuteActionRequest("GetDependentDisks", jsonInput);
        const auto response = service.RecvExecuteActionResponse();

        UNIT_ASSERT(requestReceived);
        UNIT_ASSERT_VALUES_EQUAL(returnedCode, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(expectedErrorFlags, response->GetError().GetFlags());

        if (returnedCode == S_OK) {
            NProto::TGetDependentDisksResponse output;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(), &output).ok());
            ASSERT_VECTORS_EQUAL(returnedDiskIds, output.GetDependentDiskIds());
        } else {
            UNIT_ASSERT_VALUES_EQUAL("", response->Record.GetOutput());
        }
    }

    Y_UNIT_TEST(ShouldGetDependentDevicesNoDisks)
    {
        ShouldGetDependentDevicesTest({}, S_OK, NProto::EF_NONE);
    }

    Y_UNIT_TEST(ShouldGetDependentDevicesMultipleDisks)
    {
        ShouldGetDependentDevicesTest({"disk1", "disk2"}, S_OK, NProto::EF_NONE);
    }

    Y_UNIT_TEST(ShouldGetDependentDevicesAndSilentNotFound)
    {
        ShouldGetDependentDevicesTest({}, E_NOT_FOUND, NProto::EF_SILENT);
    }

    NProto::TChangeStorageConfigResponse ExecuteChangeStorageConfig(
        NProto::TStorageServiceConfig config,
        TServiceClient& service,
        bool mergeWithConfig = false)
    {
        NProto::TChangeStorageConfigRequest request;
        request.SetDiskId("vol0");

        *request.MutableStorageConfig() = std::move(config);
        request.SetMergeWithStorageConfigFromVolumeDB(mergeWithConfig);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto jsonResponse = service.ExecuteAction(
            "changestorageconfig", buf);
        NProto::TChangeStorageConfigResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());
        return response;
    }

    void CheckStorageConfigValues(
        TVector<TString> names,
        THashMap<TString, TString> responseData,
        TServiceClient& service,
        NActors::TTestActorRuntime& runtime)
    {
        auto request = service.CreateStatVolumeRequest("vol0", names);
        service.SendRequest(MakeStorageServiceId(), std::move(request));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = service.RecvStatVolumeResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        auto mapping = response->Record.GetStorageConfigFieldsToValues();

        for (const auto& [name, value]: responseData) {
            UNIT_ASSERT_VALUES_EQUAL(mapping[name], value);
        }
    }

    Y_UNIT_TEST(ShouldChangeStorageConfig)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol0");

        CheckStorageConfigValues(
            {"CompactionRangeCountPerRun"},
            {{"CompactionRangeCountPerRun", "Default"}},
            service,
            runtime);

        {
            // Check that new config was set
            NProto::TStorageServiceConfig newConfig;
            newConfig.SetCompactionRangeCountPerRun(1000);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionRangeCountPerRun(),
                1000);
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        CheckStorageConfigValues(
            {"CompactionRangeCountPerRun"},
            {{"CompactionRangeCountPerRun", "1000"}},
            service,
            runtime);

        {
            // Check that configs are merged, when
            // MergeWithStorageConfigFromVolumeDB is true
            NProto::TStorageServiceConfig newConfig;
            newConfig.SetCompactionGarbageThreshold(10);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, true);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionRangeCountPerRun(),
                1000);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionGarbageThreshold(),
                10);

            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        CheckStorageConfigValues(
            {"CompactionRangeCountPerRun", "CompactionGarbageThreshold"},
            {
                {"CompactionRangeCountPerRun", "1000"},
                {"CompactionGarbageThreshold", "10"}
            },
            service,
            runtime);

        {
            // Check that configs aren't merged, when
            // MergeWithStorageConfigFromVolumeDB is false
            NProto::TStorageServiceConfig newConfig;
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, false);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionRangeCountPerRun(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionGarbageThreshold(),
                0);

            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

        CheckStorageConfigValues(
            {"CompactionRangeCountPerRun", "CompactionGarbageThreshold"},
            {
                {"CompactionRangeCountPerRun", "Default"},
                {"CompactionGarbageThreshold", "Default"}
            },
            service,
            runtime);
    }

    Y_UNIT_TEST(ShouldExecuteCmsAction)
    {
        TTestEnv env;
        TServiceClient service(env.GetRuntime(), SetupTestEnv(env));

        env.GetRuntime().SetEventFilter([] (auto&, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvService::EvCmsActionRequest: {
                    auto* msg = event->template Get<TEvService::TEvCmsActionRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(1, msg->Record.ActionsSize());
                    const auto& action = msg->Record.GetActions(0);
                    UNIT_ASSERT_EQUAL(NProto::TAction_EType_REMOVE_HOST, action.GetType());
                    UNIT_ASSERT_EQUAL("localhost", action.GetHost());
                    UNIT_ASSERT(!action.GetDryRun());
                    break;
                }
                case TEvService::EvCmsActionResponse: {
                    auto* msg = event->template Get<TEvService::TEvCmsActionResponse>();
                    auto& r = *msg->Record.AddActionResults();
                    *r.MutableResult() = MakeError(E_TRY_AGAIN);
                    r.SetTimeout(42);
                    r.AddDependentDisks("vol0");
                    break;
                }
            }
            return false;
        });

        NProto::TCmsActionRequest request;
        auto& action = *request.AddActions();
        action.SetType(NProto::TAction_EType_REMOVE_HOST);
        action.SetHost("localhost");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        const auto response = service.ExecuteAction("cms", buf);

        NProto::TCmsActionResponse responseProto;
        if (!google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &responseProto).ok())
        {
            UNIT_ASSERT(false);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, responseProto.ActionResultsSize());
        const auto& r = responseProto.GetActionResults(0);
        UNIT_ASSERT_VALUES_EQUAL(42, r.GetTimeout());
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, r.GetResult().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, r.DependentDisksSize());
        UNIT_ASSERT_VALUES_EQUAL("vol0", r.GetDependentDisks(0));
    }

    Y_UNIT_TEST(ShouldWaitDependentDisksToSwitchNodeTest)
    {
        struct TVolumeStructure
        {
            TString DiskId;
            int DeviceCount = 3;
            int ReplicaCount = 0;
            int NodeId = 50002;
        };
        auto getVolumeDescripriton =
            [](const TVolumeStructure& structure)
        {
            NProto::TVolume volume;
            for (int i = 0; i < structure.DeviceCount; i++) {
                auto* device = volume.AddDevices();
                device->SetNodeId(structure.NodeId);
            }
            for (int i = 0; i < structure.ReplicaCount; i++) {
                auto* replica = volume.AddReplicas();
                for (int j = 0; j < structure.DeviceCount; j++) {
                    auto* device = replica->AddDevices();
                    device->SetNodeId(structure.NodeId);
                }
            }
            volume.SetDiskId(structure.DiskId);
            return volume;
        };

        TTestEnv env(1, 1, 4, 1, MakeIntrusive<TDiskRegistryState>());
        env.GetRuntime().EnableScheduleForActor(MakeDiskRegistryProxyServiceId());
        env.GetRuntime().EnableScheduleForActor(MakeVolumeProxyServiceId());

        NProto::TStorageServiceConfig config;
        // config.SetAllocationUnitNonReplicatedSSD(100);
        config.SetWaitDependentDisksRetryRequestDelay(50);  // 50 ms.
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TVector<TString> returnedDiskIds{"nrd1", "nrd2"};
        constexpr int OldNodeId = 50001;
        auto evFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistry::EvGetDependentDisksRequest: {
                    auto* msg = event->Get<
                        TEvDiskRegistry::TEvGetDependentDisksRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        "localhost",
                        msg->Record.GetHost());
                    UNIT_ASSERT_VALUES_EQUAL("", msg->Record.GetPath());
                    break;
                }
                case TEvDiskRegistry::EvGetDependentDisksResponse: {
                    static int RequestCount = 0;
                    RequestCount++;

                    auto* msg = event->Get<
                        TEvDiskRegistry::TEvGetDependentDisksResponse>();
                    msg->Record.MutableDependentDiskIds()->Add(
                        returnedDiskIds.begin(),
                        returnedDiskIds.end());
                    // Check proper retriable errors handling.
                    msg->Record.MutableError()->SetCode(
                        RequestCount < 2 ? E_REJECTED : S_OK);
                    break;
                }
                case TEvVolume::EvGetVolumeInfoRequest: {
                    static int RequestCount = 0;
                    RequestCount++;

                    auto* msg =
                        event->Get<TEvVolume::TEvGetVolumeInfoRequest>();
                    auto response =
                        std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
                    *response->Record.MutableVolume() =
                        getVolumeDescripriton(TVolumeStructure{
                            .DiskId = msg->Record.GetDiskId(),
                            // Make sure that the actor will retry requests
                            // until the NodeId hasn't changed.
                            .NodeId =
                                (RequestCount > 3 ? OldNodeId + 1
                                                  : OldNodeId)
                            });
                    runtime.Send(new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie));
                    return true;
                }
                case TEvVolume::EvWaitReadyRequest: {
                    auto* msg = event->Get<TEvVolume::TEvWaitReadyRequest>();
                    auto response =
                        std::make_unique<TEvVolume::TEvWaitReadyResponse>();
                    *response->Record.MutableVolume() = getVolumeDescripriton(
                        TVolumeStructure{.DiskId = msg->Record.GetDiskId()});
                    runtime.Send(new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,   // flags
                        event->Cookie));
                    return true;
                }
            }
            return false;
        };
        env.GetRuntime().SetEventFilter(evFilter);

        NProto::TWaitDependentDisksToSwitchNodeRequest request;
        request.SetAgentId("localhost");
        request.SetOldNodeId(OldNodeId);
        TString jsonInput;
        UNIT_ASSERT(
            google::protobuf::util::MessageToJsonString(request, &jsonInput)
                .ok());
        service.SendExecuteActionRequest(
            "WaitDependentDisksToSwitchNode",
            jsonInput);

        const auto response = service.RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        NProto::TWaitDependentDisksToSwitchNodeResponse output;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                        response->Record.GetOutput(),
                        &output)
                        .ok());
        UNIT_ASSERT_VALUES_EQUAL(
            returnedDiskIds.size(),
            output.GetDependentDiskStates().size());
        for (const auto& state: output.GetDependentDiskStates()) {
            UNIT_ASSERT_C(
                AnyOf(
                    returnedDiskIds,
                    [&](const auto& diskId)
                    { return state.GetDiskId() == diskId; }),
                "Missing disk id: " << state.GetDiskId());
            UNIT_ASSERT_EQUAL(
                state.GetDiskState(),
                NProto::TWaitDependentDisksToSwitchNodeResponse::
                    DISK_STATE_READY);
        }
    }

    Y_UNIT_TEST(ShouldWaitDependentDisksToSwitchNodeErrorTest)
    {
        TTestEnv env(1, 1, 4, 1, MakeIntrusive<TDiskRegistryState>());

        NProto::TStorageServiceConfig config;
        // config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);
        TServiceClient service(env.GetRuntime(), nodeIdx);

        constexpr int OldNodeId = 50001;
        auto evFilter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistry::EvGetDependentDisksRequest: {
                    auto* msg = event->Get<
                        TEvDiskRegistry::TEvGetDependentDisksRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        "localhost",
                        msg->Record.GetHost());
                    UNIT_ASSERT_VALUES_EQUAL("", msg->Record.GetPath());
                    break;
                }
                case TEvDiskRegistry::EvGetDependentDisksResponse: {
                    auto* msg = event->Get<
                        TEvDiskRegistry::TEvGetDependentDisksResponse>();
                    *msg->Record.MutableError() =
                        MakeError(E_FAIL, "test error");
                    break;
                }
            }
            return false;
        };
        env.GetRuntime().SetEventFilter(evFilter);

        env.GetRuntime().SetRegistrationObserverFunc(
            [](TTestActorRuntimeBase& runtime,
               const TActorId& parentId,
               const TActorId& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        NProto::TWaitDependentDisksToSwitchNodeRequest request;
        request.SetAgentId("localhost");
        request.SetOldNodeId(OldNodeId);
        TString jsonInput;
        UNIT_ASSERT(
            google::protobuf::util::MessageToJsonString(request, &jsonInput)
                .ok());
        service.SendExecuteActionRequest(
            "WaitDependentDisksToSwitchNode",
            jsonInput);
        const auto response = service.RecvExecuteActionResponse();

        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->GetStatus());
        NProto::TWaitDependentDisksToSwitchNodeResponse output;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                        response->Record.GetOutput(),
                        &output)
                        .ok());
        UNIT_ASSERT_VALUES_EQUAL(0, output.GetDependentDiskStates().size());
    }

    NProto::TStorageServiceConfig ExecuteGetStorageConfig(
        const TString& diskId,
        TServiceClient& service)
    {
        NProto::TGetStorageConfigRequest request;
        request.SetDiskId(diskId);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto jsonResponse = service.ExecuteAction("getstorageconfig", buf);
        NProto::TStorageServiceConfig response;
        auto status = google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response);
        UNIT_ASSERT_C(status.ok(), status.message().data());
        return response;
    }

    Y_UNIT_TEST(ShouldGetStorageConfigFromNodeOrVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, config);
        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume("vol0");

        ExecuteGetStorageConfig("", service);
        // we cannot compare returned config with test config since
        // test env alters configuration passed

        {
            NProto::TStorageServiceConfig newConfig;
            newConfig.SetCompactionRangeCountPerRun(1000);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionRangeCountPerRun(),
                1000);
        }

        {
            auto response = ExecuteGetStorageConfig("vol0", service);

            UNIT_ASSERT_VALUES_EQUAL(
                response.GetCompactionRangeCountPerRun(),
                1000);
        }
    }

    Y_UNIT_TEST(ShouldCheckRange)
    {
        constexpr ui32 blockCount = 1000;

        TTestEnv env;
        NProto::TStorageServiceConfig config;
        const ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(DefaultDiskId);
        auto sessionId =
            service.MountVolume(DefaultDiskId)->Record.GetSessionId();
        service.WriteBlocks(
            DefaultDiskId,
            TBlockRange64::WithLength(0, 1024),
            sessionId,
            char(1));

        {
            NProto::TCheckRangeRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetStartIndex(0);
            request.SetBlocksCount(blockCount);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);

            const auto response = service.ExecuteAction("CheckRange", buf);
            NProto::TCheckRangeResponse checkRangeResponse;

            UNIT_ASSERT(
                google::protobuf::util::JsonStringToMessage(
                    response->Record.GetOutput(),
                    &checkRangeResponse)
                    .ok());

            UNIT_ASSERT_VALUES_EQUAL(
                blockCount,
                checkRangeResponse.GetDiskChecksums().DataSize());
        }
    }

    Y_UNIT_TEST(ShouldSetVhostDiscardEnabledFlag)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);

        auto setVhostDiscardEnabledFlagAndCheckResult = [&](bool discardEnabled)
        {
            NPrivateProto::TSetVhostDiscardFlagActionRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetVhostDiscardEnabled(discardEnabled);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("setvhostdiscardenabledflag", buf);

            auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                discardEnabled,
                volumeConfig.GetVhostDiscardEnabled());
        };

        setVhostDiscardEnabledFlagAndCheckResult(false);
        setVhostDiscardEnabledFlagAndCheckResult(false);
        setVhostDiscardEnabledFlagAndCheckResult(true);
        setVhostDiscardEnabledFlagAndCheckResult(true);
        setVhostDiscardEnabledFlagAndCheckResult(false);
    }

    Y_UNIT_TEST(ShouldValidateSetVhostDiscardEnabledFlag)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(DefaultDiskId);

        {
            // Correct ConfigVersion.
            NPrivateProto::TSetVhostDiscardFlagActionRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetVhostDiscardEnabled(false);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("setvhostdiscardenabledflag", buf);

            auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                volumeConfig.GetVhostDiscardEnabled());
        }

        {
            // Incorrect ConfigVersion.
            NPrivateProto::TSetVhostDiscardFlagActionRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(3);
            request.SetVhostDiscardEnabled(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("SetVhostDiscardEnabledFlag", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->GetStatus());

            auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                volumeConfig.GetVhostDiscardEnabled());
        }

        {
            // DiskId should be supplied.
            NPrivateProto::TSetVhostDiscardFlagActionRequest request;
            request.SetVhostDiscardEnabled(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("SetVhostDiscardEnabledFlag", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());

            auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                volumeConfig.GetVhostDiscardEnabled());
        }
    }

    Y_UNIT_TEST(ShouldListNVMeDevices)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const auto response =
            service.ExecuteAction("ListNVMeDevices", TString());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Record.GetOutput(), &value));
        const auto& devices = value["Devices"].GetArray();

        UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
        UNIT_ASSERT_VALUES_EQUAL("NVME_0", devices[0]["SerialNumber"]);
    }

    Y_UNIT_TEST(ShouldAcquireNVMeDevice)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            service.SendExecuteActionRequest("AcquireNVMeDevice", TString());
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            service.SendExecuteActionRequest(
                "AcquireNVMeDevice",
                R"({"SerialNumber":"foo"})");
            auto response = service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                R"(Disk "foo" not found)",
                response->GetErrorReason());
        }

        service.ExecuteAction("AcquireNVMeDevice", R"({"SerialNumber":"NVME_0"})");
    }

    Y_UNIT_TEST(ShouldReleaseNVMeDevice)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        {
            service.SendExecuteActionRequest("ReleaseNVMeDevice", TString());
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
        }

        {
            service.SendExecuteActionRequest(
                "ReleaseNVMeDevice",
                R"({"SerialNumber":"foo"})");
            auto response = service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(
                R"(Disk "foo" not found)",
                response->GetErrorReason());
        }

        service.ExecuteAction("ReleaseNVMeDevice", R"({"SerialNumber":"NVME_0"})");
    }

    Y_UNIT_TEST(ShouldHandleBscPipeDisconnectInGetClusterCapacity)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto makeFilter = [&] (auto eventCode) {
            return
            [&, eventCode = eventCode]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvSend: {
                        if (event->Type != eventCode) {
                            return false;
                        }
                        auto response =
                            std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
                                0,
                                TActorId{},
                                TActorId{});
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            nodeIdx);
                        return true;
                    }
                }
                return false;
            };
        };

        constexpr ui32 lostEvent[] = {
            NSysView::TEvSysView::EvGetStoragePoolsRequest,
            NSysView::TEvSysView::EvGetGroupsRequest,
            NSysView::TEvSysView::EvGetVSlotsRequest
        };

        for (auto e: lostEvent) {
            env.GetRuntime().SetEventFilter(makeFilter(e));

            service.SendExecuteActionRequest("getclustercapacity", "{}");
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldReturnClusterCapacity)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        auto response = service.ExecuteAction("getclustercapacity", "{}");
        NPrivateProto::TGetClusterCapacityResponse output;
        UNIT_ASSERT(
            google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &output)
                .ok());
        UNIT_ASSERT_VALUES_UNEQUAL(0, output.GetCapacity().size());
    }

    Y_UNIT_TEST(ShouldDescribeDisk)
    {
        auto drState = MakeIntrusive<TDiskRegistryState>();
        TTestEnv env(1, 1, 4, 1, {drState});

        NProto::TStorageServiceConfig config;
        config.SetAllocationUnitNonReplicatedSSD(100);
        ui32 nodeIdx = SetupTestEnv(env, config);

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateVolume(
            DefaultDiskId,
            100_GB / DefaultBlockSize,
            DefaultBlockSize,
            "",
            "",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

        auto* disk = drState->Disks.FindPtr(DefaultDiskId);
        UNIT_ASSERT(disk);

        TString buf;

        {
            NProto::TDescribeDiskRequest proto;
            proto.SetDiskId(DefaultDiskId);
            google::protobuf::util::MessageToJsonString(proto, &buf);
        }

        const auto response =
            service.ExecuteAction("diskregistrydescribedisk", buf);

        NProto::TDescribeDiskResponse proto;
        UNIT_ASSERT(
            google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &proto)
                .ok());
        UNIT_ASSERT_VALUES_EQUAL(disk->Devices.size(), proto.DevicesSize());
    }

    Y_UNIT_TEST(ShouldUseExactDiskIdMatchOnModifyTags)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume();

        bool exactDiskIdMatch = false;
        bool hasDiskIdInVolumeConfig = true;

        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvDescribeVolumeRequest>();
                        exactDiskIdMatch = msg.ExactDiskIdMatch;
                        break;
                    }
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        const auto& volumeConfig =
                            msg.ModifyScheme.GetAlterBlockStoreVolume()
                                .GetVolumeConfig();
                        hasDiskIdInVolumeConfig = volumeConfig.HasDiskId();
                        break;
                    }
                }

                return false;
            });

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = "a";

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("ModifyTags", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(true, exactDiskIdMatch);
        UNIT_ASSERT_VALUES_EQUAL(false, hasDiskIdInVolumeConfig);
    }

    Y_UNIT_TEST(ShouldFailModifyTagsIfOnlySecondaryDiskExists)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume(GetSecondaryDiskId(DefaultDiskId));

        {
            NPrivateProto::TModifyTagsRequest request;
            request.SetDiskId(DefaultDiskId);
            *request.AddTagsToAdd() = MakeTag("source-disk-id", "some-disk");

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("ModifyTags", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldUseExactDiskIdMatchOnRebaseVolume)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume(DefaultDiskId);
        service.CreateVolume("new-base-disk");

        bool exactDiskIdMatch = false;
        int describeCount = 0;

        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvDescribeVolumeRequest>();
                        ++describeCount;
                        // Second describe is for the volume itself (first
                        // is for base disk, which should not use
                        // ExactDiskIdMatch).
                        if (describeCount == 2) {
                            exactDiskIdMatch = msg.ExactDiskIdMatch;
                        }
                        break;
                    }
                }

                return false;
            });

        {
            NPrivateProto::TRebaseVolumeRequest rebaseReq;
            rebaseReq.SetDiskId(DefaultDiskId);
            rebaseReq.SetTargetBaseDiskId("new-base-disk");
            rebaseReq.SetConfigVersion(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(rebaseReq, &buf);
            service.ExecuteAction("rebasevolume", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(true, exactDiskIdMatch);

        auto volumeConfig = GetVolumeConfig(service, DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL("new-base-disk", volumeConfig.GetBaseDiskId());
    }

    Y_UNIT_TEST(ShouldUseExactDiskIdMatchOnFinishFillDisk)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);

        auto request = service.CreateCreateVolumeRequest();
        request->Record.SetFillGeneration(1);
        service.SendRequest(MakeStorageServiceId(), std::move(request));

        auto response = service.RecvCreateVolumeResponse();
        UNIT_ASSERT_C(response->GetStatus() == S_OK, response->GetStatus());

        bool exactDiskIdMatch = false;

        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvDescribeVolumeRequest>();
                        exactDiskIdMatch = msg.ExactDiskIdMatch;
                        break;
                    }
                }

                return false;
            });

        {
            NPrivateProto::TFinishFillDiskRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetConfigVersion(1);
            request.SetFillGeneration(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("finishfilldisk", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(true, exactDiskIdMatch);
    }

    Y_UNIT_TEST(ShouldUseExactDiskIdMatchOnSetVhostDiscardFlag)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        auto& runtime = env.GetRuntime();
        TServiceClient service(runtime, nodeIdx);
        service.CreateVolume(DefaultDiskId);

        bool exactDiskIdMatch = false;
        bool hasDiskIdInVolumeConfig = true;

        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeVolumeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvDescribeVolumeRequest>();
                        exactDiskIdMatch = msg.ExactDiskIdMatch;
                        break;
                    }
                    case TEvSSProxy::EvModifySchemeRequest: {
                        auto& msg =
                            *event->Get<TEvSSProxy::TEvModifySchemeRequest>();
                        const auto& volumeConfig =
                            msg.ModifyScheme.GetAlterBlockStoreVolume()
                                .GetVolumeConfig();
                        hasDiskIdInVolumeConfig = volumeConfig.HasDiskId();
                        break;
                    }
                }

                return false;
            });

        {
            NPrivateProto::TSetVhostDiscardFlagActionRequest request;
            request.SetDiskId(DefaultDiskId);
            request.SetVhostDiscardEnabled(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("setvhostdiscardenabledflag", buf);
        }

        UNIT_ASSERT_VALUES_EQUAL(true, exactDiskIdMatch);
        UNIT_ASSERT_VALUES_EQUAL(false, hasDiskIdInVolumeConfig);
    }

    Y_UNIT_TEST(ShouldForwardDrainDownPolicy)
    {
        TTestEnv env;
        NProto::TStorageServiceConfig config;
        ui32 nodeIdx = SetupTestEnv(env, std::move(config));

        TServiceClient service(env.GetRuntime(), nodeIdx);

        NKikimrHive::EDrainDownPolicy observedDownPolicy =
            NKikimrHive::DRAIN_POLICY_NO_DOWN;
        env.GetRuntime().SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvHive::EvDrainNode) {
                observedDownPolicy = event->Get<TEvHive::TEvDrainNode>()
                    ->Record.GetDownPolicy();
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        NPrivateProto::TDrainNodeRequest request;
        request.SetDownPolicy(
            NCloud::NProto::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART);

        TString input;
        google::protobuf::util::MessageToJsonString(request, &input);
        service.ExecuteAction("drainnode", input);

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(
                NKikimrHive::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART),
            static_cast<int>(observedDownPolicy));
    }
}

}   // namespace NCloud::NBlockStore::NStorage


template <>
void Out<TVolumeParamMap>(
    IOutputStream& out,
    const TVolumeParamMap& paramMap)
{
    out << '[';
    for (const auto& [key,value]: paramMap) {
        out << '{'
            << "Key: " << key << ", "
            << "Value: " << value.GetValue() << ", "
            << "Ttl: " << value.GetTtlMs()
            << '}';
    }
    out << ']';
}
