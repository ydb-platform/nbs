#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/ss_proxy/protos/path_description_backup.pb.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <cloud/storage/core/libs/kikimr/public.h>

#include <contrib/ydb/core/protos/flat_tx_scheme.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TPathDescriptionBackup final
    : public NActors::TActorBootstrapped<TPathDescriptionBackup>
{
private:
    const TFsPath BackupFilePath;
    const bool UseBinaryFormat = false;
    const bool ReadOnlyMode = false;

    NSSProxy::NProto::TPathDescriptionBackup BackupProto;
    const TFsPath TmpBackupFilePath;

public:
    TPathDescriptionBackup(TString backupFilePath, bool useBinaryFormat, bool readOnlyMode);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ScheduleBackup(const NActors::TActorContext& ctx);
    NProto::TError Backup(const NActors::TActorContext& ctx);

    bool LoadFromTextFormat(const NActors::TActorContext& ctx);
    bool LoadFromBinaryFormat(const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadPathDescriptionBackup(
        const TEvSSProxyPrivate::TEvReadPathDescriptionBackupRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdatePathDescriptionBackup(
        const TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBackupPathDescriptions(
        const TEvSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
