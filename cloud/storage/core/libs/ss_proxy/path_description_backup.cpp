#include "path_description_backup.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/kikimr/components.h>

#include <contrib/ydb/library/actors/core/log.h>

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

TPathDescriptionBackup::TPathDescriptionBackup(
        int logComponent,
        TString backupFilePath,
        bool readOnlyMode)
    : LogComponent(logComponent)
    , BackupFilePath(std::move(backupFilePath))
    , ReadOnlyMode(readOnlyMode)
    , TmpBackupFilePath(BackupFilePath.GetPath() + ".tmp")
{}

void TPathDescriptionBackup::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (ReadOnlyMode) {
        try {
            TFile file(BackupFilePath, OpenExisting | RdOnly | Seq);
            TUnbufferedFileInput input(file);
            MergeFromTextFormat(input, BackupProto);
            LOG_INFO_S(ctx, LogComponent,
                "PathDescriptionBackup: loaded " << file.GetLength() << " bytes");
        } catch (...) {
            ReportLoadPathDescriptionBackupFailure();
            LOG_WARN_S(ctx, LogComponent,
                "PathDescriptionBackup: can't load from file: "
                << CurrentExceptionMessage());
        }
    } else {
        ScheduleBackup(ctx);
    }

    LOG_INFO_S(ctx, LogComponent,
        "PathDescriptionBackup: started with ReadOnlyMode=" << ReadOnlyMode);
}

void TPathDescriptionBackup::ScheduleBackup(const TActorContext& ctx)
{
    ctx.Schedule(BackupInterval, new TEvents::TEvWakeup());
}

NProto::TError TPathDescriptionBackup::Backup(const TActorContext& ctx)
{
    NProto::TError error;

    try {
        TFileLock lock(TmpBackupFilePath);

        if (lock.TryAcquire()) {
            Y_DEFER {
                lock.Release();
            };
            TFileOutput output(TmpBackupFilePath);
            SerializeToTextFormat(BackupProto, output);
            TmpBackupFilePath.RenameTo(BackupFilePath);
        } else {
            auto message = TStringBuilder()
                << "failed to acquire lock on file: " << TmpBackupFilePath;
            error = MakeError(E_IO, std::move(message));
        }
    } catch (...) {
        error = MakeError(E_FAIL, CurrentExceptionMessage());
    }

    if (SUCCEEDED(error.GetCode())) {
        LOG_DEBUG_S(ctx, LogComponent,
            "PathDescriptionBackup: backup completed");
    } else {
        ReportBackupPathDescriptionsFailure();

        LOG_ERROR_S(ctx, LogComponent,
            "PathDescriptionBackup: backup failed: "
            << error);

        try {
            TmpBackupFilePath.DeleteIfExists();
        } catch (...) {
            LOG_WARN_S(ctx, LogComponent,
                "PathDescriptionBackup: failed to delete temporary file: "
                << CurrentExceptionMessage());
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

void TPathDescriptionBackup::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Backup(ctx);
    ScheduleBackup(ctx);
}

void TPathDescriptionBackup::HandleReadPathDescriptionBackup(
    const TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxyPrivate::TEvReadPathDescriptionBackupResponse;

    auto* msg = ev->Get();

    bool found = false;
    NKikimrSchemeOp::TPathDescription pathDescription;

    {
        auto it = BackupProto.GetData().find(msg->Path);
        if (it != BackupProto.GetData().end()) {
            found = true;
            pathDescription = it->second;
        }
    }

    std::unique_ptr<TResponse> response;

    if (found) {
        LOG_DEBUG_S(ctx, LogComponent,
            "PathDescriptionBackup: found data for path " << msg->Path);
        response = std::make_unique<TResponse>(
            std::move(msg->Path), std::move(pathDescription));
    } else {
        LOG_DEBUG_S(ctx, LogComponent,
            "PathDescriptionBackup: no data for path " << msg->Path);
        response = std::make_unique<TResponse>(MakeError(E_NOT_FOUND));
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPathDescriptionBackup::HandleUpdatePathDescriptionBackup(
    const TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& data = *BackupProto.MutableData();
    data[msg->Path] = std::move(msg->PathDescription);

    LOG_DEBUG_S(ctx, LogComponent,
        "PathDescriptionBackup: updated data for path " << msg->Path);
}

void TPathDescriptionBackup::HandleBackupPathDescriptions(
    const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvBackupPathDescriptionsResponse;

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

STFUNC(TPathDescriptionBackup::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest, HandleReadPathDescriptionBackup);
        HFunc(TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest, HandleUpdatePathDescriptionBackup);
        HFunc(TEvSSProxy::TEvBackupPathDescriptionsRequest, HandleBackupPathDescriptions);

        default:
            HandleUnexpectedEvent(ev, LogComponent);
            break;
    }
}

}   // namespace NCloud::NStorage
