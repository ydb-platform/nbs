#pragma once

#include "test_env_state.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/ss_proxy/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tx_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TTestEnv
{
private:
    const ui32 DomainUid;
    const TString DomainName;
    const ui32 StaticNodeCount;
    const ui32 DynamicNodeCount;
    const TTestEnvState State;
    NKikimr::TTestBasicRuntime Runtime;

    IFileIOServicePtr FileIOService;

    ui32 NextDynamicNode;

    TVector<ui32> GroupIds;

    ITraceSerializerPtr TraceSerializer;

public:
    TTestEnv(
        ui32 staticNodes = 1,
        ui32 dynamicNodes = 1,
        ui32 nchannels = 4,
        ui32 ngroups = 1,
        TTestEnvState state = {},
        NKikimr::NFake::TCaches cachesConfig = {});

    ~TTestEnv();

    NActors::TTestActorRuntime& GetRuntime()
    {
        return Runtime;
    }

    IFileIOServicePtr GetFileIOService()
    {
        return FileIOService;
    }

    ui64 GetHive();
    ui64 GetSchemeShard();

    const auto& GetGroupIds() const
    {
        return GroupIds;
    }

    ui64 AllocateTxId();

    void CreateSubDomain(const TString& name);

    ui32 CreateBlockStoreNode(
        const TString& name,
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig);

    ui32 CreateBlockStoreNode(
        const TString& name,
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes);

    ui32 CreateBlockStoreNode(
        const TString& name,
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        NYdbStats::IYdbVolumesStatsUploaderPtr ydbStatsUploader,
        TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes);

    TString UpdatePrivateCacheSize(ui64 tabletId, ui64 cacheSize);
    ui64 GetPrivateCacheSize(ui64 tabletId);

private:
    void SetupLogging();
    void SetupDomain(NKikimr::TAppPrepare& app);
    void SetupChannelProfiles(NKikimr::TAppPrepare& app, ui32 nchannels = 4);

    void BootTablets();
    void BootStandardTablet(
        ui64 tabletId,
        NKikimr::TTabletTypes::EType type,
        ui32 nodeIdx = 0);

    void SetupStorage(ui32 ngroups = 1);

    void SetupLocalServices();
    void SetupLocalService(ui32 nodeIdx);

    void SetupProxies();
    void SetupTicketParser(ui32 nodeIdx);
    void SetupTxProxy(ui32 nodeIdx);
    void SetupCompileService(ui32 nodeIdx);

    void InitSchemeShard();

    void WaitForSchemeShardTx(ui64 txId);
};

}   // namespace NCloud::NBlockStore::NStorage
