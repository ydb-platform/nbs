#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/volume/actors/follower_disk_actor.h>
#include <cloud/blockstore/libs/storage/volume/actors/volume_as_partition_actor.h>
#include <cloud/blockstore/libs/storage/volume/model/helpers.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <util/generic/algorithm.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class ESourceLifecycle
{
    GcBeforeLink,
    UnmountDuringCopy,
    RebootBeforeAndAfterGc,
    RebootNeverMounted,
};

struct TCopyLifecycleEnv
{
    TTestEnv Env{1, 1, 4};
    ui32 Node = 0;
    std::unique_ptr<TServiceClient> Service;
    ui64 SourceTabletId = 0;
    TVector<ui64> SourcePartitionIds;
    TActorId CopierActor;
    TActorId CopyPartitionActor;
    TActorId SourceVolumeActor;
    ui32 SourcePartitionStops = 0;
    bool HoldGc = false;
    bool HoldCopy = false;
    TVector<std::unique_ptr<IEventHandle>> GcReports;
    std::unique_ptr<IEventHandle> CopyRequest;

    explicit TCopyLifecycleEnv(ui32 partitions = 1, ui64 blocks = 128)
    {
        if (partitions > 1) {
            blocks = Max<ui64>(blocks, 4096 * partitions);
        }
        auto& runtime = Env.GetRuntime();
        auto previous = runtime.SetRegistrationObserverFunc({});
        runtime.SetRegistrationObserverFunc(
            [this, previous = std::move(previous)](
                TTestActorRuntimeBase& runtime, const TActorId& parent,
                const TActorId& actor)
            {
                if (previous) {
                    previous(runtime, parent, actor);
                }
                auto* instance = runtime.FindActor(actor);
                if (dynamic_cast<TFollowerDiskActor*>(instance)) {
                    CopierActor = actor;
                    CopyPartitionActor = {};
                }
                if (dynamic_cast<TVolumeAsPartitionActor*>(instance) &&
                    parent == CopierActor)
                {
                    CopyPartitionActor = actor;
                }
                if (dynamic_cast<TFollowerDiskActor*>(instance) ||
                    dynamic_cast<TVolumeAsPartitionActor*>(instance))
                {
                    runtime.EnableScheduleForActor(actor);
                }
            });
        auto scheduled = runtime.SetScheduledEventFilter({});
        runtime.SetScheduledEventFilter(
            [scheduled = std::move(scheduled)](TTestActorRuntimeBase& runtime,
                                               TAutoPtr<IEventHandle>& event,
                                               TDuration delay,
                                               TInstant& deadline)
            {
                if (event->GetTypeRewrite() ==
                    TEvService::EvDestroyVolumeRequest) {
                    return false;
                }
                return scheduled ? scheduled(runtime, event, delay, deadline)
                                 : false;
            });
        runtime.SetEventFilter(
            [this](auto&, TAutoPtr<IEventHandle>& event)
            {
                const auto type = event->GetTypeRewrite();
                if (type ==
                        NCloud::NStorage::TEvHiveProxy::EvBootExternalRequest &&
                    IsIn(
                        SourcePartitionIds,
                        event
                            ->Get<NCloud::NStorage::TEvHiveProxy::
                                      TEvBootExternalRequest>()
                            ->TabletId))
                {
                    if (SourceVolumeActor != event->Sender) {
                        GcReports.clear();
                        SourceVolumeActor = event->Sender;
                    }
                }
                if (type ==
                        NPartition::TEvPartition::EvGarbageCollectorCompleted &&
                    IsIn(
                        SourcePartitionIds,
                        event
                            ->Get<NPartition::TEvPartition::
                                      TEvGarbageCollectorCompleted>()
                            ->TabletId))
                {
                    if (SourceVolumeActor &&
                        SourceVolumeActor != event->GetRecipientRewrite())
                    {
                        return false;
                    }
                    SourceVolumeActor = event->GetRecipientRewrite();
                    if (HoldGc) {
                        const auto tablet =
                            event
                                ->Get<NPartition::TEvPartition::
                                          TEvGarbageCollectorCompleted>()
                                ->TabletId;
                        EraseIf(
                            GcReports,
                            [=](const auto& report)
                            {
                                return report
                                           ->template Get<
                                               NPartition::TEvPartition::
                                                   TEvGarbageCollectorCompleted>()
                                           ->TabletId == tablet;
                            });
                        GcReports.emplace_back(event.Release());
                        return true;
                    }
                }
                if (type == TEvBootstrapper::EvStop &&
                    event->Sender == SourceVolumeActor)
                {
                    ++SourcePartitionStops;
                }
                if (HoldCopy && !CopyRequest &&
                    event->Sender == CopyPartitionActor &&
                    (type == TEvService::EvWriteBlocksRequest ||
                     type == TEvService::EvZeroBlocksRequest))
                {
                    const auto& headers =
                        type == TEvService::EvWriteBlocksRequest
                            ? event->Get<TEvService::TEvWriteBlocksRequest>()
                                  ->Record.GetHeaders()
                            : event->Get<TEvService::TEvZeroBlocksRequest>()
                                  ->Record.GetHeaders();
                    if (headers.GetClientId() == CopyVolumeClientId) {
                        CopyRequest.reset(event.Release());
                        return true;
                    }
                }
                return false;
            });

        NProto::TStorageServiceConfig config;
        config.SetUseDirectCopyRange(false);
        config.SetMultipartitionVolumesEnabled(true);
        auto* bandwidth = config.AddLinkedDiskFillBandwidth();
        bandwidth->SetReadBandwidth(64 * 1024 * 1024);
        bandwidth->SetWriteBandwidth(64 * 1024 * 1024);
        bandwidth->SetReadIoDepth(1);
        bandwidth->SetWriteIoDepth(1);
        Node = SetupTestEnv(Env, std::move(config));
        Service = std::make_unique<TServiceClient>(runtime, Node);
        // Explicit partitions are aligned to the service model's minimum size.
        if (partitions > 1) {
            blocks = Max<ui64>(blocks, 4096 * partitions);
        }
        auto source = Service->CreateCreateVolumeRequest("source", blocks);
        source->Record.SetPartitionsCount(partitions);
        Service->SendRequest(MakeStorageServiceId(), std::move(source));
        UNIT_ASSERT(!HasError(Service->RecvCreateVolumeResponse()->GetError()));
        const auto sourceDescription = Service->DescribeVolume("source");
        const auto sourceBlocks =
            sourceDescription->Record.GetVolume().GetBlocksCount();
        auto target =
            Service->CreateCreateVolumeRequest("target", sourceBlocks);
        target->Record.SetPartitionsCount(1);
        target->Record.SetTagsStr(MakeTag("source-disk-id", "source"));
        Service->SendRequest(MakeStorageServiceId(), std::move(target));
        UNIT_ASSERT(!HasError(Service->RecvCreateVolumeResponse()->GetError()));
        Service->SendRequest(
            MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>("source"));
        const auto description =
            Service->RecvResponse<TEvSSProxy::TEvDescribeVolumeResponse>();
        UNIT_ASSERT(!HasError(description->GetError()));
        const auto& volume =
            description->PathDescription.GetBlockStoreVolumeDescription();
        SourceTabletId = volume.GetVolumeTabletId();
        UNIT_ASSERT_VALUES_EQUAL(partitions, volume.PartitionsSize());
        for (const auto& partition: volume.GetPartitions()) {
            SourcePartitionIds.push_back(partition.GetTabletId());
        }
    }

    ~TCopyLifecycleEnv()
    {
        Env.GetRuntime().SetEventFilter([](auto&, auto&) { return false; });
        Env.GetRuntime().SetRegistrationObserverFunc({});
    }

    NProto::TGetLinkStatusResponse Status()
    {
        NProto::TGetLinkStatusRequest request;
        request.SetLeaderDiskId("source");
        request.SetFollowerDiskId("target");
        google::protobuf::util::JsonPrintOptions options;
        options.preserve_proto_field_names = true;
        TString input;
        UNIT_ASSERT(
            google::protobuf::util::MessageToJsonString(request, &input,
                                                        options)
                .ok());
        const auto response = Service->ExecuteAction("getlinkstatus", input);
        NProto::TGetLinkStatusResponse status;
        UNIT_ASSERT(
            google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(), &status)
                .ok());
        return status;
    }

    void Advance()
    {
        auto& runtime = Env.GetRuntime();
        runtime.UpdateCurrentTime(
            runtime.GetCurrentTime() + TDuration::Seconds(2));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
    }

    void WaitForCopyRequest()
    {
        for (ui32 attempt = 0; attempt < 10 && !CopyRequest; ++attempt) {
            Advance();
        }
        UNIT_ASSERT_C(CopyRequest, Status().ShortDebugString());
    }

    void ReleaseGc()
    {
        HoldGc = false;
        auto reports = std::move(GcReports);
        for (auto& report: reports) {
            Env.GetRuntime().SendAsync(report.release(), Node);
        }
    }

    void WaitForGcReports()
    {
        for (ui32 attempt = 0;
             attempt < 10 && GcReports.size() < SourcePartitionIds.size();
             ++attempt)
        {
            Advance();
        }
        UNIT_ASSERT_VALUES_EQUAL(SourcePartitionIds.size(), GcReports.size());
    }

    void ReleaseGcReport(ui64 tabletId)
    {
        auto it = FindIf(
            GcReports,
            [=](const auto& report)
            {
                return report
                           ->template Get<NPartition::TEvPartition::
                                              TEvGarbageCollectorCompleted>()
                           ->TabletId == tabletId;
            });
        UNIT_ASSERT(it != GcReports.end());
        auto report = std::move(*it);
        GcReports.erase(it);
        HoldGc = false;
        Env.GetRuntime().SendAsync(report.release(), Node);
        Advance();
        HoldGc = true;
    }

    void ReleaseCopy()
    {
        HoldCopy = false;
        if (CopyRequest) {
            Env.GetRuntime().SendAsync(CopyRequest.release(), Node);
        }
    }

    void FailCopy()
    {
        UNIT_ASSERT(CopyRequest);
        UNIT_ASSERT(
            CopyRequest->GetTypeRewrite() == TEvService::EvWriteBlocksRequest);
        auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(
            MakeError(E_ARGUMENT, "injected destination failure"));
        Env.GetRuntime().SendAsync(
            new IEventHandle(
                CopyRequest->Sender,
                CopyRequest->Recipient,
                response.release(),
                0,
                CopyRequest->Cookie),
            Node);
        CopyRequest.reset();
        HoldCopy = false;
    }

    void Reboot()
    {
        GcReports.clear();   // Captured reports belong to the old volume actor.
        NKikimr::RebootTablet(Env.GetRuntime(), SourceTabletId,
                              Service->GetSender(), Node);
    }

    void CheckCompleted(bool dataPresent)
    {
        for (ui32 attempt = 0; attempt < 40; ++attempt) {
            if (Status().GetStatus() == NProto::LINK_STATUS_COMPLETED) {
                break;
            }
            Advance();
        }
        const auto status = Status();
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<ui32>(NProto::LINK_STATUS_COMPLETED),
            static_cast<ui32>(status.GetStatus()), status.ShortDebugString());
        Service->SendDescribeVolumeRequest("source");
        UNIT_ASSERT(IsNotFoundSchemeShardError(
            Service->RecvDescribeVolumeResponse()->GetError()));
        const auto mounted = Service->MountVolume("target");
        const auto response =
            Service->ReadBlocks("target", 0, mounted->Record.GetSessionId());
        if (dataPresent) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 'a'),
                response->Record.GetBlocks().GetBuffers(0));
        } else {
            for (const auto& buffer: response->Record.GetBlocks().GetBuffers())
            {
                for (const char byte: buffer) {
                    UNIT_ASSERT_VALUES_EQUAL(0, byte);
                }
            }
        }
    }
};

void CheckSourceLifecycle(ESourceLifecycle lifecycle)
{
    TCopyLifecycleEnv env;
    const bool dataPresent = lifecycle != ESourceLifecycle::RebootNeverMounted;
    TString session;
    if (dataPresent) {
        session = env.Service->MountVolume("source")->Record.GetSessionId();
        env.Service->WriteBlocks("source", TBlockRange64::MakeOneBlock(0),
                                 session, 'a');
    }
    if (lifecycle == ESourceLifecycle::GcBeforeLink) {
        env.HoldGc = true;
        env.Service->UnmountVolume("source", session);
        env.Reboot();
    }
    if (lifecycle == ESourceLifecycle::RebootNeverMounted) {
        env.Reboot();
    }
    env.HoldCopy = dataPresent;
    env.Service->CreateVolumeLink("source", "target");
    if (dataPresent) {
        env.WaitForCopyRequest();
        if (lifecycle != ESourceLifecycle::GcBeforeLink) {
            env.Service->UnmountVolume("source", session);
        }
        if (lifecycle == ESourceLifecycle::RebootBeforeAndAfterGc) {
            env.CopyRequest.reset();   // The old in-flight attempt was lost.
            env.HoldGc = true;
            env.Reboot();
            env.WaitForCopyRequest();
        }
        if (lifecycle == ESourceLifecycle::GcBeforeLink ||
            lifecycle == ESourceLifecycle::RebootBeforeAndAfterGc)
        {
            env.WaitForGcReports();
        }
        env.ReleaseGc();
        env.Advance();
        if (lifecycle == ESourceLifecycle::RebootBeforeAndAfterGc) {
            // Serialize a read after the GC completion transactions.
            UNIT_ASSERT(
                env.Status().GetStatus() == NProto::LINK_STATUS_PREPARING);
            env.CopyRequest.reset();
            env.Reboot();
            env.WaitForCopyRequest();
        }
        env.ReleaseCopy();
    }
    env.CheckCompleted(dataPresent);
}

void CheckRepeatedGcCycles()
{
    TCopyLifecycleEnv env(2, 4096);
    const auto session =
        env.Service->MountVolume("source")->Record.GetSessionId();
    env.Service->WriteBlocks("source", TBlockRange64::MakeOneBlock(0), session,
                             'a');
    env.HoldCopy = true;
    env.Service->CreateVolumeLink("source", "target");
    env.WaitForCopyRequest();
    env.Service->UnmountVolume("source", session);
    // Unmount can replace the volume owner; fail a request from the new copier.
    env.CopyRequest.reset();
    env.HoldGc = true;
    env.Reboot();
    env.WaitForCopyRequest();
    env.FailCopy();
    for (ui32 attempt = 0;
         attempt < 40 && env.Status().GetStatus() != NProto::LINK_STATUS_ERROR;
         ++attempt)
    {
        env.Advance();
    }
    UNIT_ASSERT(env.Status().GetStatus() == NProto::LINK_STATUS_ERROR);

    // An errored link does not keep the source in USE after a reboot.
    env.Reboot();
    env.WaitForGcReports();
    const auto owner = env.SourceVolumeActor;
    env.ReleaseGcReport(env.SourcePartitionIds[0]);
    env.GcReports.clear();

    // Removing the errored link restarts partitions in the same GC-only mode.
    env.Service->DestroyVolumeLink("source", "target");
    env.WaitForGcReports();
    UNIT_ASSERT_VALUES_EQUAL(owner, env.SourceVolumeActor);
    const auto stops = env.SourcePartitionStops;
    // Reverse the report order in the new cycle: old A must not satisfy it.
    env.ReleaseGcReport(env.SourcePartitionIds[1]);
    UNIT_ASSERT_VALUES_EQUAL(stops, env.SourcePartitionStops);
    env.ReleaseGcReport(env.SourcePartitionIds[0]);
    for (ui32 attempt = 0; attempt < 10 && env.SourcePartitionStops == stops;
         ++attempt)
    {
        env.Advance();
    }
    UNIT_ASSERT(env.SourcePartitionStops > stops);
}

}   // namespace

Y_UNIT_TEST_SUITE(TCopyLifecycleTest)
{
    Y_UNIT_TEST(ShouldKeepCopyRunningAfterGcWithoutMountedSource)
    {
        CheckSourceLifecycle(ESourceLifecycle::GcBeforeLink);
    }

    Y_UNIT_TEST(ShouldContinueCopyWhenLastClientUnmounts)
    {
        CheckSourceLifecycle(ESourceLifecycle::UnmountDuringCopy);
    }

    Y_UNIT_TEST(ShouldResumeCopyAfterRebootsBeforeAndAfterGc)
    {
        CheckSourceLifecycle(ESourceLifecycle::RebootBeforeAndAfterGc);
    }

    Y_UNIT_TEST(ShouldStartCopyAfterNeverMountedSourceReboot)
    {
        CheckSourceLifecycle(ESourceLifecycle::RebootNeverMounted);
    }

    Y_UNIT_TEST(ShouldSeparateGcReportsBetweenPartitionStarts)
    {
        CheckRepeatedGcCycles();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
