#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMarkReplacementDevicesActor final
    : public TActorBootstrapped<TMarkReplacementDevicesActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr RequestInfo;
    const NProto::TFinishMigrationRequest Request;

    NProto::TError Error;
    int PendingOperations = 0;

public:
    TMarkReplacementDevicesActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TFinishMigrationRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    void MarkReplacementDevices(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleResponse(
        const TEvDiskRegistry::TEvMarkReplacementDeviceResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TMarkReplacementDevicesActor::TMarkReplacementDevicesActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TFinishMigrationRequest request)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TMarkReplacementDevicesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    MarkReplacementDevices(ctx);
}

void TMarkReplacementDevicesActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvDiskRegistry::TEvFinishMigrationResponse>(
        std::move(Error));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

void TMarkReplacementDevicesActor::MarkReplacementDevices(const TActorContext& ctx)
{
    PendingOperations = Request.MigrationsSize();

    ui64 cookie = 0;
    for (const auto& m: Request.GetMigrations()) {
        auto request =
            std::make_unique<TEvDiskRegistry::TEvMarkReplacementDeviceRequest>();
        request->Record.SetDiskId(Request.GetDiskId());
        request->Record.SetDeviceId(m.GetTargetDeviceId());
        // Mark that the device has completed the replacement.
        // Explicitly setting the default value for the proto field for clarity.
        request->Record.SetIsReplacement(false);
        NCloud::Send(
            ctx,
            Owner,
            std::move(request),
            cookie++
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMarkReplacementDevicesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeTabletIsDeadError(E_REJECTED, __LOCATION__);
    ReplyAndDie(ctx);
}

void TMarkReplacementDevicesActor::HandleResponse(
    const TEvDiskRegistry::TEvMarkReplacementDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    --PendingOperations;

    Y_ABORT_UNLESS(PendingOperations >= 0);

    if (HasError(ev->Get()->Record)) {
        Error = std::move(*ev->Get()->Record.MutableError());
    }

    if (!PendingOperations) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMarkReplacementDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvDiskRegistry::TEvMarkReplacementDeviceResponse, HandleResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_REGISTRY_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleFinishMigration(
    const TEvDiskRegistry::TEvFinishMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(FinishMigration);

    auto& record = ev->Get()->Record;

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received FinishMigration request: DiskId=%s, Migrations=%d",
        TabletID(),
        record.GetDiskId().c_str(),
        record.MigrationsSize());

    auto requestInfo = CreateRequestInfo<TEvDiskRegistry::TFinishMigrationMethod>(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    auto diskId = record.GetDiskId();
    auto migrations = record.GetMigrations();

    if (State->IsMasterDisk(record.GetDiskId())) {
        TString replicaId;
        ui32 migrationCount = 0;
        for (const auto& m: migrations) {
            auto anotherReplicaId = State->FindReplicaByMigration(
                diskId,
                m.GetSourceDeviceId(),
                m.GetTargetDeviceId());

            if (replicaId && anotherReplicaId != replicaId) {
                // currently this shouldn't happen but if we support batch
                // migration in the future our code should not break
                // dumb solution would be to simply report a critical event
                // and just process the finished migrations belonging to
                // the first detected replica - it will force other replicas
                // to migrate again, which is inefficient but which won't
                // break anything

                ReportUnexpectedBatchMigration();

                break;
            }

            replicaId = anotherReplicaId;
            ++migrationCount;
        }

        if (replicaId) {
            diskId = replicaId;
            migrations.Truncate(migrationCount);
        } else {
            // no replica migrations found => this FinishMigration request
            // actually means that fresh device replication has finished

            auto actor = NCloud::Register<TMarkReplacementDevicesActor>(
                ctx,
                SelfId(),
                std::move(requestInfo),
                std::move(record)
            );
            Actors.insert(actor);

            return;
        }
    }

    ExecuteTx<TFinishMigration>(
        ctx,
        std::move(requestInfo),
        std::move(diskId),
        std::move(migrations),
        ctx.Now()
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareFinishMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TFinishMigration& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteFinishMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TFinishMigration& args)
{
    TDiskRegistryDatabase db(tx.DB);
    for (auto& x: args.Migrations) {
        bool updated = false;
        auto error = State->FinishDeviceMigration(
            db,
            args.DiskId,
            x.GetSourceDeviceId(),
            x.GetTargetDeviceId(),
            args.Timestamp,
            &updated);
        Y_UNUSED(updated);

        if (HasError(error)) {
            LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
                "FinishDeviceMigration error: %s. DiskId=%s Source=%s Target=%s",
                FormatError(error).c_str(),
                args.DiskId.c_str(),
                x.GetSourceDeviceId().c_str(),
                x.GetTargetDeviceId().c_str());
        } else {
            LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
                "FinishDeviceMigration succeeded. DiskId=%s Source=%s Target=%s",
                args.DiskId.c_str(),
                x.GetSourceDeviceId().c_str(),
                x.GetTargetDeviceId().c_str());
        }

        if (!HasError(args.Error)) {
            args.Error = error;
        }
    }
}

void TDiskRegistryActor::CompleteFinishMigration(
    const TActorContext& ctx,
    TTxDiskRegistry::TFinishMigration& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "FinishMigration complete. DiskId=%s Migrations=%d",
        args.DiskId.c_str(),
        args.Migrations.size());

    ReallocateDisks(ctx);
    NotifyUsers(ctx);
    PublishDiskStates(ctx);
    SecureErase(ctx);

    auto response = std::make_unique<TEvDiskRegistry::TEvFinishMigrationResponse>(
        std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::StartMigration(const NActors::TActorContext& ctx)
{
    if (StartMigrationInProgress) {
        return;
    }

    if (State->IsMigrationListEmpty()) {
        return;
    }

    StartMigrationInProgress = true;

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvStartMigrationRequest>();

    auto deadline = Min(StartMigrationStartTs, ctx.Now()) + TDuration::Seconds(5);
    if (deadline > ctx.Now()) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Scheduled device migration, now: %lu, deadline: %lu",
            TabletID(),
            ctx.Now().MicroSeconds(),
            deadline.MicroSeconds());

        ctx.Schedule(
            deadline,
            std::make_unique<IEventHandle>(ctx.SelfID, ctx.SelfID, request.release()));
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Sending device migration request",
            TabletID());

        NCloud::Send(ctx, ctx.SelfID, std::move(request));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleStartMigration(
    const TEvDiskRegistryPrivate::TEvStartMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(StartMigration);

    StartMigrationStartTs = ctx.Now();

    ExecuteTx<TStartMigration>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TStartMigrationMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        )
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareStartMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TStartMigration& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteStartMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TStartMigration& args)
{
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);

    for (const auto& [diskId, deviceId]: State->BuildMigrationList()) {
        const auto result = State->StartDeviceMigration(ctx.Now(), db, diskId, deviceId);

        if (HasError(result)) {
            LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
                "[%lu] Start migration failed. DiskId=%s DeviceId=%s Error=%s",
                TabletID(),
                diskId.Quote().c_str(),
                deviceId.Quote().c_str(),
                FormatError(result.GetError()).Quote().c_str()
            );
        } else {
            const auto& target = result.GetResult();
            LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
                "[%lu] Start migration success. DiskId=%s DeviceId=%s TargetId={ %s %u %lu(%lu) }",
                TabletID(),
                diskId.Quote().c_str(),
                deviceId.Quote().c_str(),
                target.GetDeviceUUID().Quote().c_str(),
                target.GetBlockSize(),
                target.GetBlocksCount(),
                target.GetUnadjustedBlockCount()
            );
        }
    }
}

void TDiskRegistryActor::CompleteStartMigration(
    const TActorContext& ctx,
    TTxDiskRegistry::TStartMigration& args)
{
    ReallocateDisks(ctx);
    NotifyUsers(ctx);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvDiskRegistryPrivate::TEvStartMigrationResponse>());
}

void TDiskRegistryActor::HandleStartMigrationResponse(
    const TEvDiskRegistryPrivate::TEvStartMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    StartMigrationInProgress = false;
    StartMigration(ctx);
}


////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleStartForceMigration(
    const TEvDiskRegistry::TEvStartForceMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(StartForceMigration);

    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received StartForceMigrationRequest request",
        TabletID());

    if (msg->Record.GetSourceDiskId().empty() ||
        msg->Record.GetSourceDeviceId().empty() ||
        msg->Record.GetTargetDeviceId().empty() )
    {
        auto response = std::make_unique<
            TEvDiskRegistry::TEvStartForceMigrationResponse>();
        *response->Record.MutableError() = MakeError(
            E_ARGUMENT,
            TStringBuilder() <<
            "Failed to start disk migration " <<
            msg->Record.GetSourceDiskId().Quote() <<
            " device " << msg->Record.GetSourceDeviceId().Quote() <<
            " to device " << msg->Record.GetTargetDeviceId().Quote());
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ExecuteTx<TStartForceMigration>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TStartMigrationMethod>(
            ev->Sender,
            ev->Cookie,
            msg->CallContext
        ),
        msg->Record.GetSourceDiskId(),
        msg->Record.GetSourceDeviceId(),
        msg->Record.GetTargetDeviceId());
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareStartForceMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TStartForceMigration& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteStartForceMigration(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TStartForceMigration& args)
{
    Y_UNUSED(args);

    TDiskRegistryDatabase db(tx.DB);

    const auto result = State->StartDeviceMigration(
        ctx.Now(),
        db,
        args.SourceDiskId,
        args.SourceDeviceId,
        args.TargetDeviceId);

    if (HasError(result)) {
        args.Error = result.GetError();
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Failed to start forced migration. DiskId=%s DeviceId=%s Error=%s",
            TabletID(),
            args.SourceDiskId.Quote().c_str(),
            args.SourceDeviceId.Quote().c_str(),
            FormatError(result.GetError()).Quote().c_str()
        );
    } else {
        const auto& target = result.GetResult();
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Start force migration success. DiskId=%s DeviceId=%s TargetId={ %s %u %lu(%lu) }",
            TabletID(),
            args.SourceDiskId.Quote().c_str(),
            args.SourceDeviceId.Quote().c_str(),
            target.GetDeviceUUID().Quote().c_str(),
            target.GetBlockSize(),
            target.GetBlocksCount(),
            target.GetUnadjustedBlockCount()
        );
    }
}

void TDiskRegistryActor::CompleteStartForceMigration(
    const TActorContext& ctx,
    TTxDiskRegistry::TStartForceMigration& args)
{
    ReallocateDisks(ctx);
    NotifyUsers(ctx);

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvDiskRegistry::TEvStartForceMigrationResponse>(args.Error));
}

}   // namespace NCloud::NBlockStore::NStorage
