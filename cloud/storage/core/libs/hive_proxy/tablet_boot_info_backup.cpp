#include "tablet_boot_info_backup.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/kikimr/components.h>

#include <ydb/core/base/tablet.h>

#include <library/cpp/actors/core/log.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

namespace NCloud::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration BackupInterval = TDuration::Seconds(10);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTabletBootInfoBackup::TTabletBootInfoBackup(
        int logComponent,
        TString backupFilePath,
        bool readOnlyMode)
    : LogComponent(logComponent)
    , BackupFilePath(std::move(backupFilePath))
    , ReadOnlyMode(readOnlyMode)
    , TmpBackupFilePath(BackupFilePath.GetPath() + ".tmp")
{}

void TTabletBootInfoBackup::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (ReadOnlyMode) {
        try {
            TFile file(BackupFilePath, OpenExisting | RdOnly | Seq);
            TUnbufferedFileInput input(file);
            MergeFromTextFormat(input, BackupProto);
            LOG_INFO_S(ctx, LogComponent,
                "TabletBootInfoBackup: loaded " << file.GetLength() << " bytes");
        } catch (...) {
            ReportLoadTabletBootInfoBackupFailure();
            LOG_WARN_S(ctx, LogComponent,
                "TabletBootInfoBackup: can't load from file: "
                << CurrentExceptionMessage());
        }
    } else {
        ScheduleBackup(ctx);
    }

    LOG_INFO_S(ctx, LogComponent,
        "TabletBootInfoBackup: started with ReadOnlyMode=" << ReadOnlyMode);
}

void TTabletBootInfoBackup::ScheduleBackup(const TActorContext& ctx)
{
    ctx.Schedule(BackupInterval, new TEvents::TEvWakeup());
}

NProto::TError TTabletBootInfoBackup::Backup(const TActorContext& ctx)
{
    NProto::TError error;

    try {
        TFileLock lock(TmpBackupFilePath);

        if (lock.TryAcquire()) {
            TFileOutput output(TmpBackupFilePath);
            SerializeToTextFormat(BackupProto, output);
            TmpBackupFilePath.RenameTo(BackupFilePath);
        } else {
            auto message = TStringBuilder()
                << "failed to acquire lock on file: " << TmpBackupFilePath;
            error = MakeError(E_FAIL, std::move(message));
        }
    } catch (...) {
        error = MakeError(E_FAIL, CurrentExceptionMessage());
    }

    if (SUCCEEDED(error.GetCode())) {
        LOG_DEBUG_S(ctx, LogComponent, "TabletBootInfoBackup: backup completed");
    } else {
        ReportBackupTabletBootInfosFailure();

        LOG_ERROR_S(ctx, LogComponent,
            "TabletBootInfoBackup: backup failed: "
            << error);

        try {
            TmpBackupFilePath.DeleteIfExists();
        } catch (...) {
            LOG_WARN_S(ctx, LogComponent,
                "TabletBootInfoBackup: failed to delete temporary file: "
                << CurrentExceptionMessage());
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBootInfoBackup::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Backup(ctx);
    ScheduleBackup(ctx);
}

void TTabletBootInfoBackup::HandleReadTabletBootInfoBackup(
    const TEvHiveProxyPrivate::TEvReadTabletBootInfoBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxyPrivate::TEvReadTabletBootInfoBackupResponse;

    auto* msg = ev->Get();

    std::unique_ptr<TResponse> response;

    auto it = BackupProto.GetData().find(msg->TabletId);
    if (it == BackupProto.GetData().end()) {
        LOG_DEBUG_S(ctx, LogComponent,
            "TabletBootInfoBackup: no data for tablet " << msg->TabletId);
        response = std::make_unique<TResponse>(MakeError(E_NOT_FOUND));
    } else {
        LOG_DEBUG_S(ctx, LogComponent,
            "TabletBootInfoBackup: read data for tablet " << msg->TabletId);

        auto storageInfo = it->second.GetStorageInfo();
        auto suggestedGeneration = it->second.GetSuggestedGeneration();
        response = std::make_unique<TResponse>(
            NKikimr::TabletStorageInfoFromProto(std::move(storageInfo)),
            suggestedGeneration);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TTabletBootInfoBackup::HandleUpdateTabletBootInfoBackup(
    const TEvHiveProxyPrivate::TEvUpdateTabletBootInfoBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    NHiveProxy::NProto::TTabletBootInfo info;
    NKikimr::TabletStorageInfoToProto(
        *msg->StorageInfo, info.MutableStorageInfo());
    info.SetSuggestedGeneration(msg->SuggestedGeneration);

    auto& data = *BackupProto.MutableData();
    data[msg->StorageInfo->TabletID] = info;

    LOG_DEBUG_S(ctx, LogComponent,
        "TabletBootInfoBackup: updated data for tablet "
            << msg->StorageInfo->TabletID);
}

void TTabletBootInfoBackup::HandleBackupTabletBootInfos(
    const TEvHiveProxy::TEvBackupTabletBootInfosRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxy::TEvBackupTabletBootInfosResponse;

    NProto::TError error;
    if (ReadOnlyMode) {
        error = MakeError(E_PRECONDITION_FAILED, "backup file is read-only");
    } else {
        error = Backup(ctx);
    }

    auto response = std::make_unique<TResponse>(std::move(error));
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TTabletBootInfoBackup::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvHiveProxyPrivate::TEvReadTabletBootInfoBackupRequest, HandleReadTabletBootInfoBackup);
        HFunc(TEvHiveProxyPrivate::TEvUpdateTabletBootInfoBackupRequest, HandleUpdateTabletBootInfoBackup);
        HFunc(TEvHiveProxy::TEvBackupTabletBootInfosRequest, HandleBackupTabletBootInfos);
        default:
            HandleUnexpectedEvent(ev, LogComponent);
            break;
    }
}

}   // namespace NCloud::NStorage
