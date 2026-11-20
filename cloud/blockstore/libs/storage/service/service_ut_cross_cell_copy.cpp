#include "service_ut.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/actors/follower_disk_actor.h>
#include <cloud/blockstore/libs/storage/volume/actors/volume_as_partition_actor.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>
#include <cloud/blockstore/libs/storage/testlib/cell_manager_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

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
        auto& runtime = Env.GetRuntime();
        if (data) {
            auto previous = runtime.SetRegistrationObserverFunc({});
            runtime.SetRegistrationObserverFunc(
                [previous = std::move(previous)](
                    TTestActorRuntimeBase& runtime,
                    const TActorId& parent,
                    const TActorId& actor)
                {
                    if (previous) {
                        previous(runtime, parent, actor);
                    }
                    auto* instance = runtime.FindActor(actor);
                    if (dynamic_cast<TFollowerDiskActor*>(instance) ||
                        dynamic_cast<TVolumeAsPartitionActor*>(instance))
                    {
                        runtime.EnableScheduleForActor(actor);
                    }
                });
            auto scheduled = runtime.SetScheduledEventFilter({});
            runtime.SetScheduledEventFilter(
                [scheduled = std::move(scheduled)](
                    TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& event,
                    TDuration delay,
                    TInstant& deadline)
                {
                    if (event->GetTypeRewrite() ==
                            TEvCellProxy::EvExecuteActionRequest &&
                        event->Get<TEvCellProxy::TEvExecuteActionRequest>()
                                ->Record.GetAction() == "destroylinksource")
                    {
                        return false;
                    }
                    return scheduled
                        ? scheduled(runtime, event, delay, deadline) : false;
                });
        }
        auto createNode = [&](const TString& cell)
        {
            Env.CreateSubDomain(cell);
            NProto::TStorageServiceConfig config;
            config.SetSchemeShardDir("/local/" + cell);
            config.SetUseDirectCopyRange(false);
            if (data) {
                auto* bandwidth = config.AddLinkedDiskFillBandwidth();
                bandwidth->SetReadBandwidth(64 * 1024 * 1024);
                bandwidth->SetWriteBandwidth(64 * 1024 * 1024);
                bandwidth->SetReadIoDepth(1);
                bandwidth->SetWriteIoDepth(1);
            }
            return Env.CreateBlockStoreNode(
                cell, CreateTestStorageConfig(std::move(config)),
                CreateTestDiagnosticsConfig(),
                NYdbStats::CreateVolumesStatsUploaderStub(),
                CreateManuallyPreemptedVolumes(), cell);
        };
        NodeA = createNode("cell-a");
        NodeB = createNode("cell-b");
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

    void WaitForCopy(const TString& source, const TString& target)
    {
        auto& runtime = Env.GetRuntime();
        for (ui32 attempt = 0; attempt < 40; ++attempt) {
            const auto status = Status(*A, source, target);
            UNIT_ASSERT_VALUES_UNEQUAL(
                static_cast<ui32>(NProto::LINK_STATUS_ERROR),
                static_cast<ui32>(status.GetStatus()));
            if (status.GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                return;
            }
            runtime.UpdateCurrentTime(
                runtime.GetCurrentTime() + TDuration::Seconds(2));
            runtime.DispatchEvents(
                TDispatchOptions(), TDuration::MilliSeconds(10));
        }
        const auto status = Status(*A, source, target);
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<ui32>(NProto::LINK_STATUS_COMPLETED),
            static_cast<ui32>(status.GetStatus()),
            status.ShortDebugString());
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

enum class ECopySourceMount
{
    Mounted,
    Unmounted,
    NeverMounted,
};

void CopyAndReadBack(
    const TString& source,
    const TString& target,
    ECopySourceMount mount = ECopySourceMount::Mounted)
{
    TCopyCells cells(true);
    cells.A->CreateVolume(source, 128);
    cells.B->CreateVolume(target, 128);
    cells.B->AddTags(target, TVector{MakeTag("source-disk-id", "cell-a/" + source)});
    if (mount != ECopySourceMount::NeverMounted) {
        const auto mounted = cells.A->MountVolume(source);
        const auto session = mounted->Record.GetSessionId();
        cells.A->WriteBlocks(source, TBlockRange64::MakeOneBlock(0), session, 'a');
        cells.A->WriteBlocks(source, TBlockRange64::MakeOneBlock(63), session, 'z');
        cells.A->ZeroBlocks(source, 1, session);
        if (mount == ECopySourceMount::Unmounted) {
            cells.A->UnmountVolume(source, session);
        }
    }
    cells.A->CreateVolumeLink(source, target, "cell-b");

    cells.WaitForCopy(source, target);
    cells.A->SendDescribeVolumeRequest(source);
    const auto gone = cells.A->RecvDescribeVolumeResponse();
    UNIT_ASSERT(IsNotFoundSchemeShardError(gone->GetError()));
    const auto targetMount = cells.B->MountVolume(target);
    UNIT_ASSERT(!targetMount->Record.GetVolume().GetPrincipalDiskId());
    const auto targetSession = targetMount->Record.GetSessionId();
    const auto first = cells.B->ReadBlocks(target, 0, targetSession);
    const auto last = cells.B->ReadBlocks(target, 63, targetSession);
    if (mount == ECopySourceMount::NeverMounted) {
        for (const auto* response: {first.get(), last.get()}) {
            for (const auto& buffer: response->Record.GetBlocks().GetBuffers()) {
                for (const char byte: buffer) {
                    UNIT_ASSERT_VALUES_EQUAL(0, byte);
                }
            }
        }
    } else {
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'a'),
            first->Record.GetBlocks().GetBuffers(0));
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'z'),
            last->Record.GetBlocks().GetBuffers(0));
    }
    const auto finalStatus = cells.Status(*cells.B, source, target);
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<ui32>(NProto::LINK_STATUS_COMPLETED),
        static_cast<ui32>(finalStatus.GetStatus()));
}


void CheckCrossCellUnmountDuringCopy(bool reboot)
{
    TCopyCells cells(true);
    cells.A->CreateVolume("source", 128);
    cells.B->CreateVolume("target", 128);
    cells.B->AddTags("target", TVector{MakeTag("source-disk-id", "cell-a/source")});
    const auto mounted = cells.A->MountVolume("source");
    const auto session = mounted->Record.GetSessionId();
    cells.A->WriteBlocks("source", TBlockRange64::MakeOneBlock(0), session, 'a');
    cells.A->SendRequest(MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("source"));
    const auto described = cells.A->RecvResponse<TEvSSProxy::TEvDescribeVolumeResponse>();
    UNIT_ASSERT(!HasError(described->GetError()));
    const auto tabletId = described->PathDescription
        .GetBlockStoreVolumeDescription().GetVolumeTabletId();
    auto& runtime = cells.Env.GetRuntime();
    std::unique_ptr<IEventHandle> held;
    bool captured = false;
    runtime.SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& event)
    {
        if (!captured && event->GetTypeRewrite() == TEvService::EvWriteBlocksLocalRequest &&
            event->Get<TEvService::TEvWriteBlocksLocalRequest>()
                ->Record.GetHeaders().GetCopyLinkUUID())
        {
            captured = true;
            held.reset(event.Release());
            return true;
        }
        return false;
    });
    cells.A->CreateVolumeLink("source", "target", "cell-b");
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return captured; };
    runtime.DispatchEvents(options, TDuration::Seconds(10));
    UNIT_ASSERT(held);
    cells.A->UnmountVolume("source", session);
    if (reboot) {
        held.reset(); // Lose the old request while restarting its owner.
        NKikimr::RebootTablet(runtime, tabletId, cells.A->GetSender(), cells.NodeA);
    } else {
        runtime.SendAsync(held.release(), cells.NodeB);
    }
    runtime.SetEventFilter([](auto&, auto&) { return false; });
    cells.WaitForCopy("source", "target");
    const auto targetMount = cells.B->MountVolume("target");
    const auto response = cells.B->ReadBlocks(
        "target", 0, targetMount->Record.GetSessionId());
    UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'a'),
        response->Record.GetBlocks().GetBuffers(0));
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

    Y_UNIT_TEST(ShouldMirrorForegroundWritesAndZeroesDuringCopy)
    {
        TCopyCells cells(true);
        cells.A->CreateVolume("disk", 4096);
        cells.B->CreateVolume("disk", 4096);
        cells.B->AddTags("disk", TVector{MakeTag("source-disk-id", "cell-a/disk")});
        const auto mounted = cells.A->MountVolume("disk");
        const auto session = mounted->Record.GetSessionId();
        cells.A->WriteBlocks("disk", TBlockRange64::MakeOneBlock(3000), session, 'a');
        cells.A->WriteBlocks("disk", TBlockRange64::MakeOneBlock(3001), session, 'b');
        auto& runtime = cells.Env.GetRuntime();
        std::unique_ptr<IEventHandle> held;
        bool captured = false;
        runtime.SetEventFilter([&](auto&, TAutoPtr<IEventHandle>& event)
        {
            const auto type = event->GetTypeRewrite();
            if (!captured &&
                (type == TEvService::EvWriteBlocksLocalRequest ||
                 type == TEvService::EvZeroBlocksRequest))
            {
                const auto& headers =
                    type == TEvService::EvWriteBlocksLocalRequest
                        ? event->Get<TEvService::TEvWriteBlocksLocalRequest>()
                              ->Record.GetHeaders()
                        : event->Get<TEvService::TEvZeroBlocksRequest>()
                              ->Record.GetHeaders();
                if (headers.GetCopyLinkUUID()) {
                    captured = true;
                    held.reset(event.Release());
                    return true;
                }
            }
            return false;
        });
        cells.A->CreateVolumeLink("disk", "disk", "cell-b");
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return captured; };
        runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT(held);
        // These blocks are outside the first migration range. Their mirror
        // writes must reach the destination while that range is still in flight.
        cells.A->WriteBlocks("disk", TBlockRange64::MakeOneBlock(3000), session, 'f');
        cells.A->ZeroBlocks("disk", 3001, session);
        runtime.SendAsync(held.release(), cells.NodeB);
        runtime.SetEventFilter([](auto&, auto&) { return false; });
        cells.WaitForCopy("disk", "disk");
        const auto targetMount = cells.B->MountVolume("disk");
        const auto targetSession = targetMount->Record.GetSessionId();
        const auto written = cells.B->ReadBlocks("disk", 3000, targetSession);
        UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'f'),
            written->Record.GetBlocks().GetBuffers(0));
        const auto zeroed = cells.B->ReadBlocks("disk", 3001, targetSession);
        for (const auto& buffer: zeroed->Record.GetBlocks().GetBuffers()) {
            for (const char byte: buffer) {
                UNIT_ASSERT_VALUES_EQUAL(0, byte);
            }
        }
    }

    Y_UNIT_TEST(ShouldAllowCellInSourceTagWithoutRelaxingOtherTags)
    {
        TCopyCells cells;
        cells.B->CreateVolume("disk", 128);
        const TVector<TString> sourceTags = {
            MakeTag("source-disk-id", "cell-a/disk")};
        cells.B->AddTags("disk", sourceTags);
        cells.B->RemoveTags("disk", sourceTags);

        cells.B->SendAddTagsRequest(
            "disk", TVector{MakeTag("owner", "cell-a/disk")});
        const auto response = cells.B->RecvAddTagsResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCopyAfterSourceUnmount)
    {
        CopyAndReadBack("source", "target", ECopySourceMount::Unmounted);
    }

    Y_UNIT_TEST(ShouldCopyNeverMountedSource)
    {
        CopyAndReadBack("source", "target", ECopySourceMount::NeverMounted);
    }

    Y_UNIT_TEST(ShouldContinueCrossCellCopyWhenLastClientUnmounts)
    {
        CheckCrossCellUnmountDuringCopy(false);
    }

    Y_UNIT_TEST(ShouldResumeCrossCellCopyAfterUnmountedSourceReboot)
    {
        CheckCrossCellUnmountDuringCopy(true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
