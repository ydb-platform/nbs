#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>

#include <util/system/file_lock.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void WriteFile(TString filePath, TString data, const TActorContext& ctx)
{
    TFsPath tmpFilePath = filePath + ".tmp";

    NProto::TError error;

    try {
        TFileLock lock(tmpFilePath);

        if (lock.TryAcquire()) {
            Y_DEFER
            {
                lock.Release();
            };

            TFile file(tmpFilePath, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
            TFileOutput(file).Write(std::move(data));

            tmpFilePath.RenameTo(filePath);
        } else {
            auto message = TStringBuilder()
                << "failed to acquire lock on file: " << tmpFilePath;

            error = MakeError(E_IO, std::move(message));
        }
    } catch (...) {
        error = MakeError(E_FAIL, CurrentExceptionMessage());
    }

    if (HasError(error)) {
        LOG_ERROR_S(
                ctx,
            TBlockStoreComponents::SERVICE,
            TStringBuilder()
                << "Failed to write manually preempted volumes: "
                << error);

        ReportManuallyPreemptedVolumesFileError(error.GetMessage());

        try {
            tmpFilePath.DeleteIfExists();
        } catch (...) {
            LOG_WARN_S(
                ctx,
                TBlockStoreComponents::SERVICE,
                "ServiceActorSyncManuallyPreemptedVolumes: failed to delete "
                "temporary file: "
                    << CurrentExceptionMessage());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSyncManuallyPreemptedVolumesActor final
    : public TActorBootstrapped<TSyncManuallyPreemptedVolumesActor>
{
private:
    const TString FilePath;
    const TString Data;

public:
    TSyncManuallyPreemptedVolumesActor(
        TString filePath,
        TString data);

    void Bootstrap(const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSyncManuallyPreemptedVolumesActor::TSyncManuallyPreemptedVolumesActor(
        TString filePath,
        TString data)
    : FilePath(std::move(filePath))
    , Data(std::move(data))
{}

void TSyncManuallyPreemptedVolumesActor::Bootstrap(const TActorContext& ctx)
{
    WriteFile(std::move(FilePath), std::move(Data), ctx);

    using TResponse = TEvServicePrivate::TEvSyncManuallyPreemptedVolumesComplete;

    auto response = std::make_unique<TResponse>();
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(response));

    Die(ctx);
}

/////////////////////////////-///////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::RunManuallyPreemptedVolumesSync(const TActorContext& ctx)
{
    if (IsSyncManuallyPreemptedVolumesRunning ||
        Config->GetDisableManuallyPreemptedVolumesTracking())
    {
        return;
    }

    auto actor = std::make_unique<TSyncManuallyPreemptedVolumesActor>(
        Config->GetManuallyPreemptedVolumesFile(),
        State.GetManuallyPreemptedVolumes()->Serialize());
    ctx.Register(
        actor.release(),
        TMailboxType::Revolving,
        AppData(ctx)->IOPoolId);
    IsSyncManuallyPreemptedVolumesRunning = true;

    HasPendingManuallyPreemptedVolumesUpdate = false;
}

void TServiceActor::HandleUpdateManuallyPreemptedVolume(
    const TEvServicePrivate::TEvUpdateManuallyPreemptedVolume::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (State.GetIsManuallyPreemptedVolumesTrackingDisabled()) {
        return;
    }

    const auto* msg = ev->Get();

    HasPendingManuallyPreemptedVolumesUpdate = true;

    auto volumes = State.GetManuallyPreemptedVolumes();
    if (msg->PreemptionSource == NProto::SOURCE_MANUAL) {
        volumes->AddVolume(msg->DiskId, ctx.Now());
    } else {
        volumes->RemoveVolume(msg->DiskId);
    }

    RunManuallyPreemptedVolumesSync(ctx);
}

void TServiceActor::HandleSyncManuallyPreemptedVolumesComplete(
    const TEvServicePrivate::TEvSyncManuallyPreemptedVolumesComplete::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (HasPendingManuallyPreemptedVolumesUpdate &&
        !Config->GetDisableManuallyPreemptedVolumesTracking())
    {
        auto actor = std::make_unique<TSyncManuallyPreemptedVolumesActor>(
            Config->GetManuallyPreemptedVolumesFile(),
            State.GetManuallyPreemptedVolumes()->Serialize());
        ctx.Register(
            actor.release(),
            TMailboxType::Revolving,
            AppData(ctx)->IOPoolId);

        HasPendingManuallyPreemptedVolumesUpdate = false;
        return;
    }
    IsSyncManuallyPreemptedVolumesRunning = false;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
