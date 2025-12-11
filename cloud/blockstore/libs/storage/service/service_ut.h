#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>
#include <cloud/blockstore/libs/storage/testlib/service_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/config/features.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig();

ui32 SetupTestEnv(TTestEnv& env);
ui32 SetupTestEnv(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    NProto::TFeaturesConfig featuresConfig = {});

ui32 SetupTestEnvWithMultipleMount(TTestEnv& env, TDuration inactivateTimeout);

ui32 SetupTestEnvWithAllowVersionInModifyScheme(TTestEnv& env);

ui32 SetupTestEnvWithManuallyPreemptedVolumes(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    TVector<std::pair<TString, TInstant>> volumes);

ui32 SetupTestEnvWithManuallyPreemptedVolumes(
    TTestEnv& env,
    NProto::TStorageServiceConfig storageServiceConfig,
    TManuallyPreemptedVolumesPtr manuallyPreemptedVolumes);

TString GetBlockContent(char fill, size_t size = DefaultBlockSize);

NKikimrBlockStore::TVolumeConfig GetVolumeConfig(
    TServiceClient& service,
    const TString& diskId);

}   // namespace NCloud::NBlockStore::NStorage
