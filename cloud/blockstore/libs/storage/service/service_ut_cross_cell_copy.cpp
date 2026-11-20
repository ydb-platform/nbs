#include "service_ut.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/cell_manager_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

struct TCopyCells
{
    TTestEnv Env{1, 2, 4};
    ui32 NodeA;
    ui32 NodeB;
    std::unique_ptr<TServiceClient> A;
    std::unique_ptr<TServiceClient> B;

    explicit TCopyCells(bool data = false)
    {
        auto createNode = [&](const TString& cell)
        {
            Env.CreateSubDomain(cell);
            NProto::TStorageServiceConfig config;
            config.SetSchemeShardDir("/local/" + cell);
            config.SetUseDirectCopyRange(false);
            return Env.CreateBlockStoreNode(
                cell, CreateTestStorageConfig(std::move(config)),
                CreateTestDiagnosticsConfig(),
                NYdbStats::CreateVolumesStatsUploaderStub(),
                CreateManuallyPreemptedVolumes(), cell);
        };
        NodeA = createNode("cell-a");
        NodeB = createNode("cell-b");
        auto& runtime = Env.GetRuntime();
        A = std::make_unique<TServiceClient>(runtime, NodeA);
        B = std::make_unique<TServiceClient>(runtime, NodeB);
        for (auto* service: {A.get(), B.get()}) {
            service->SendRequest(MakeCellProxyServiceId(),
                std::make_unique<TEvCellProxy::TEvInitializeRequest>(
                    CreateCellManagerMock(runtime,
                        {{"cell-a", NodeA}, {"cell-b", NodeB}}, data),
                    std::make_shared<NClient::TClientAppConfig>()));
            const auto response =
                service->RecvResponse<TEvCellProxy::TEvInitializeResponse>();
            UNIT_ASSERT_C(!HasError(response->GetError()),
                FormatError(response->GetError()));
        }
    }

    static TString Json(const google::protobuf::Message& record)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.preserve_proto_field_names = true;
        TString json;
        UNIT_ASSERT(google::protobuf::util::MessageToJsonString(
            record, &json, options).ok());
        return json;
    }

    static NProto::TGetLinkStatusRequest Identity(
        const TString& source,
        const TString& target,
        const TString& uuid = {})
    {
        NProto::TGetLinkStatusRequest request;
        request.SetLeaderDiskId(source);
        request.SetLeaderCellId("cell-a");
        request.SetFollowerDiskId(target);
        request.SetFollowerCellId("cell-b");
        request.SetLinkUUID(uuid);
        return request;
    }

    NProto::TGetLinkStatusResponse Status(
        TServiceClient& service,
        const TString& source,
        const TString& target,
        const TString& uuid = {})
    {
        const auto response = service.ExecuteAction(
            "getlinkstatus", Json(Identity(source, target, uuid)));
        NProto::TGetLinkStatusResponse result;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            response->Record.GetOutput(), &result).ok());
        return result;
    }

    void FollowerAction(
        const TString& source,
        const TString& target,
        const TString& uuid,
        NProto::ELinkAction action = NProto::LINK_ACTION_CREATE)
    {
        NProto::TUpdateLinkOnFollowerRequest request;
        request.SetDiskId(target);
        request.SetLeaderDiskId(source);
        request.SetLeaderCellId("cell-a");
        request.SetFollowerCellId("cell-b");
        request.SetLinkUUID(uuid);
        request.SetAction(action);
        B->ExecuteAction("updatelinkonfollower", Json(request));
    }

    ui32 CopyWrite(
        const TString& disk,
        const TString& uuid,
        bool trusted = true)
    {
        auto request = B->CreateWriteBlocksRequest(
            disk, TBlockRange64::MakeOneBlock(0), {}, 'x');
        auto* headers = request->Record.MutableHeaders();
        headers->SetClientId(TString(CopyVolumeClientId));
        headers->SetCellId("cell-b");
        headers->SetCopyLinkUUID(uuid);
        headers->SetExactDiskIdMatch(true);
        // Simulate only the ingress trust boundary; Service and Volume execute.
        headers->MutableInternal()->SetIsCellCopyRequest(trusted);
        B->SendRequest(MakeStorageServiceId(), std::move(request));
        return B->RecvWriteBlocksResponse()->GetStatus();
    }
};

void CopyAndReadBack(const TString& source, const TString& target)
{
    TCopyCells cells(true);
    cells.A->CreateVolume(source, 128);
    cells.B->CreateVolume(target, 128);
    cells.B->AddTags(target, {MakeTag("source-disk-id", "cell-a/" + source)});
    const auto mounted = cells.A->MountVolume(source);
    const auto session = mounted->Record.GetSessionId();
    cells.A->WriteBlocks(source, TBlockRange64::MakeOneBlock(0), session, 'a');
    cells.A->WriteBlocks(source, TBlockRange64::MakeOneBlock(63), session, 'z');
    cells.A->ZeroBlocks(source, 1, session);
    cells.A->UnmountVolume(source, session);
    cells.A->CreateVolumeLink(source, target, "cell-b");

    bool completed = false;
    for (ui32 attempt = 0; attempt < 30; ++attempt) {
        const auto status = cells.Status(*cells.A, source, target);
        UNIT_ASSERT_VALUES_UNEQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_ERROR),
            static_cast<ui32>(status.GetStatus()));
        if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
            completed = true;
            break;
        }
        cells.Env.GetRuntime().DispatchEvents(
            TDispatchOptions(), TDuration::Seconds(1));
    }
    UNIT_ASSERT_C(completed, "Cross-cell copy did not finish");
    cells.A->SendDescribeVolumeRequest(source);
    const auto gone = cells.A->RecvDescribeVolumeResponse();
    UNIT_ASSERT(IsNotFoundSchemeShardError(gone->GetError()));
    const auto targetMount = cells.B->MountVolume(target);
    UNIT_ASSERT(!targetMount->Record.GetVolume().GetPrincipalDiskId());
    const auto targetSession = targetMount->Record.GetSessionId();
    const auto first = cells.B->ReadBlocks(target, 0, targetSession);
    const auto last = cells.B->ReadBlocks(target, 63, targetSession);
    UNIT_ASSERT_VALUES_EQUAL(
        TString(DefaultBlockSize, 'a'), first->Record.GetBlocks().GetBuffers(0));
    UNIT_ASSERT_VALUES_EQUAL(
        TString(DefaultBlockSize, 'z'), last->Record.GetBlocks().GetBuffers(0));
    const auto finalStatus = cells.Status(*cells.B, source, target);
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<ui32>(NProto::LINK_STATUS_COMPLETED),
        static_cast<ui32>(finalStatus.GetStatus()));
}

}   // namespace

Y_UNIT_TEST_SUITE(TCrossCellCopyTest)
{
    Y_UNIT_TEST(ShouldQueryFollowerAfterSourceDisappears)
    {
        TCopyCells cells;
        cells.B->CreateVolume("source"); // unrelated shadow of the old source
        cells.B->CreateVolume("target");
        cells.FollowerAction("source", "target", "link-1");
        const auto status = cells.Status(*cells.A, "source", "target");
        UNIT_ASSERT_VALUES_EQUAL("link-1", status.GetLinkUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::LINK_STATUS_PREPARING),
            static_cast<ui32>(status.GetStatus()));
    }

    Y_UNIT_TEST(ShouldQuerySameDiskIdInAnotherCell)
    {
        TCopyCells cells;
        cells.B->CreateVolume("disk");
        cells.FollowerAction("disk", "disk", "same-name-link");
        UNIT_ASSERT_VALUES_EQUAL(
            "same-name-link", cells.Status(*cells.A, "disk", "disk").GetLinkUUID());
    }

    Y_UNIT_TEST(ShouldRejectCopyWithWrongUUID)
    {
        TCopyCells cells;
        cells.B->CreateVolume("target");
        cells.FollowerAction("source", "target", "current-link");
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED, cells.CopyWrite("target", "old-link"));
    }

    Y_UNIT_TEST(ShouldRejectCopyWithoutTrustedIngress)
    {
        TCopyCells cells;
        cells.B->CreateVolume("target");
        cells.FollowerAction("source", "target", "current-link");
        UNIT_ASSERT_VALUES_EQUAL(
            E_UNAUTHORIZED, cells.CopyWrite("target", "current-link", false));
    }

    Y_UNIT_TEST(ShouldRejectOldCopyAfterLinkRecreation)
    {
        TCopyCells cells;
        cells.B->CreateVolume("target");
        cells.FollowerAction("source", "target", "old-link");
        cells.FollowerAction("source", "target", "old-link", NProto::LINK_ACTION_DESTROY);
        cells.FollowerAction("source", "target", "new-link");
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED, cells.CopyWrite("target", "old-link"));
    }

    Y_UNIT_TEST(ShouldRejectCopyAfterPromotion)
    {
        TCopyCells cells;
        cells.B->CreateVolume("target");
        cells.FollowerAction("source", "target", "link");
        cells.FollowerAction("source", "target", "link", NProto::LINK_ACTION_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED, cells.CopyWrite("target", "link"));
    }

    Y_UNIT_TEST(ShouldNotDeleteReplacementWithoutExpectedSourceLink)
    {
        TCopyCells cells;
        cells.A->CreateVolume("source");
        cells.A->SendExecuteActionRequest("destroylinksource",
            TCopyCells::Json(TCopyCells::Identity("source", "target", "old-link")));
        const auto response = cells.A->RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, response->GetStatus());
        cells.A->DescribeVolume("source");
    }

    Y_UNIT_TEST(ShouldTreatMissingSourceCleanupAsIdempotent)
    {
        TCopyCells cells;
        cells.A->SendExecuteActionRequest("destroylinksource",
            TCopyCells::Json(TCopyCells::Identity("source", "target", "link")));
        const auto response = cells.A->RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCopyAndDeleteSourceWithSameDiskId)
    {
        CopyAndReadBack("disk", "disk");
    }

    Y_UNIT_TEST(ShouldCopyAndDeleteSourceWithDifferentDiskIds)
    {
        CopyAndReadBack("source-copy", "target");
    }
}

}   // namespace NCloud::NBlockStore::NStorage
