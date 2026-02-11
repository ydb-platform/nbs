#include "tablet_boot_info_backup.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/kikimr/components.h>

#include <contrib/ydb/core/base/tablet.h>
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

TTabletBootInfoBackup::TTabletBootInfoBackup(
        int logComponent,
        TString backupFilePath,
        bool useBinaryFormat,
        bool readOnlyMode)
    : LogComponent(logComponent)
    , BackupFilePath(std::move(backupFilePath))
    , UseBinaryFormat(useBinaryFormat)
    , ReadOnlyMode(readOnlyMode)
    , TmpBackupFilePath(BackupFilePath.GetPath() + ".tmp")
{}

void TTabletBootInfoBackup::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    InitialBackupProto.emplace();

    // Load backup even if in read-only mode to warm up BS group connections.
    if (!LoadFromBinaryFormat(ctx) && !LoadFromTextFormat(ctx)) {
        LOG_WARN_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: can't load backup file: "
                << BackupFilePath.GetPath().Quote());
        InitialBackupProto.reset();
    }

    if (!ReadOnlyMode) {
        ScheduleBackup(ctx);
    } else if (InitialBackupProto) {
        BackupProto = std::move(*InitialBackupProto);
        InitialBackupProto.reset();
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
    Y_DEBUG_ABORT_UNLESS(!ReadOnlyMode);

    if (!BackupProtoHasChanged) {
        return MakeError(S_FALSE, "backup file is not changed");
    }
    BackupProtoHasChanged = false;

    // We don't need this anymore, because backup file will be overwritten.
    InitialBackupProto.reset();

    NProto::TError error;

    try {
        TFileLock lock(TmpBackupFilePath);

        if (lock.TryAcquire()) {
            Y_DEFER {
                lock.Release();
            };

            if (UseBinaryFormat) {
                TOFStream output(TmpBackupFilePath);
                BackupProto.SerializeToArcadiaStream(&output);
            } else {
                TFileOutput output(TmpBackupFilePath);
                SerializeToTextFormat(BackupProto, output);
            }

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
            "TabletBootInfoBackup: backup completed");
    } else {
        // We should retry the backup in case of failure.
        BackupProtoHasChanged = true;

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

bool TTabletBootInfoBackup::LoadFromTextFormat(const TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        LogComponent,
        "TabletBootInfoBackup: loading from text format: "
            << BackupFilePath.GetPath().Quote());
    try {
        TInstant start = TInstant::Now();
        MergeFromTextFormat(BackupFilePath, *InitialBackupProto);

        LOG_INFO_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: loading from text format finished "
                << FormatDuration(TInstant::Now() - start));
        return true;
    } catch (...) {
        InitialBackupProto.emplace();
        LOG_WARN_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: can't load text format file: "
                << CurrentExceptionMessage());
    }
    return false;
}

bool TTabletBootInfoBackup::LoadFromBinaryFormat(const TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        LogComponent,
        "TabletBootInfoBackup: loading from binary format: "
            << BackupFilePath.GetPath().Quote());
    try {
        TInstant start = TInstant::Now();
        TFile file(BackupFilePath, OpenExisting | RdOnly | Seq);
        TUnbufferedFileInput input(file);
        const bool success =
            InitialBackupProto->MergeFromString(input.ReadAll());

        LOG_WARN_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: loading from binary format finished with "
                << (success ? "success in " : "failure in ")
                << FormatDuration(TInstant::Now() - start));

        return success;
    } catch (...) {
        InitialBackupProto.emplace();
        LOG_WARN_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: can't load from binary format: "
                << CurrentExceptionMessage());
    }
    return false;
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

    std::optional<NHiveProxy::NProto::TTabletBootInfo> tabletBootInfo;
    {
        const auto& backupProto = InitialBackupProto.value_or(BackupProto);
        const auto it = backupProto.GetData().find(msg->TabletId);
        if (it != backupProto.GetData().end()) {
            tabletBootInfo = it->second;
        }
    }

    std::unique_ptr<TResponse> response;

    if (tabletBootInfo) {
        LOG_DEBUG_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: found data for tablet " << msg->TabletId);

        response = std::make_unique<TResponse>(
            NKikimr::TabletStorageInfoFromProto(
                tabletBootInfo->GetStorageInfo()),
            tabletBootInfo->GetSuggestedGeneration());
    } else {
        LOG_DEBUG_S(
            ctx,
            LogComponent,
            "TabletBootInfoBackup: no data for tablet " << msg->TabletId);
        response = std::make_unique<TResponse>(MakeError(E_NOT_FOUND));
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TTabletBootInfoBackup::HandleUpdateTabletBootInfoBackup(
    const TEvHiveProxyPrivate::TEvUpdateTabletBootInfoBackupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    Y_DEBUG_ABORT_UNLESS(msg->StorageInfo);

    NHiveProxy::NProto::TTabletBootInfo tabletBootInfo;
    NKikimr::TabletStorageInfoToProto(
        *msg->StorageInfo,
        tabletBootInfo.MutableStorageInfo());
    tabletBootInfo.SetSuggestedGeneration(msg->SuggestedGeneration);

    BackupProtoHasChanged = true;
    auto& data = *BackupProto.MutableData();
    data[msg->StorageInfo->TabletID] = std::move(tabletBootInfo);

    LOG_DEBUG_S(
        ctx,
        LogComponent,
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

void TTabletBootInfoBackup::HandleListTabletBootInfoBackups(
    const TEvHiveProxy::TEvListTabletBootInfoBackupsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    TVector<TTabletBootInfo> tabletBootInfos;
    const auto& backupProto = InitialBackupProto.value_or(BackupProto);
    for (const auto& [_, tabletBootInfo]: backupProto.GetData()) {
        tabletBootInfos.emplace_back(
            tabletBootInfo.GetStorageInfo(),
            tabletBootInfo.GetSuggestedGeneration());
    }

    auto response =
        std::make_unique<TEvHiveProxy::TEvListTabletBootInfoBackupsResponse>(
            std::move(tabletBootInfos));
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
        HFunc(TEvHiveProxy::TEvListTabletBootInfoBackupsRequest, HandleListTabletBootInfoBackups);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NStorage
