#pragma once

#include <cloud/blockstore/config/diagnostics.pb.h>
#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TPartitionClient;

constexpr ui32 DataChannelOffset = 3;

struct TTestPartitionInfo
{
    TString DiskId = "test";
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    ui64 TabletId = TestTabletId;
    ui64 BaseTabletId = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
    TMaybe<ui32> MaxBlocksInBlob;
    ui32 BlockSize = DefaultBlockSize;
    ui32 MixedChannelCount = 0;
};

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize);

TString GetBlocksContent(
    char fill = 0,
    ui32 blockCount = 1,
    size_t blockSize = DefaultBlockSize);

TString GetBlockContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response);

TString GetBlocksContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response);

ui64 GetFreshBlobsCount(TPartitionClient& partition);

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig(
    NProto::TDiagnosticsConfig config = {});

void InitLogSettings(NActors::TTestActorRuntime& runtime);

void InitTestActorRuntime(
    NActors::TTestActorRuntime& runtime,
    const NProto::TStorageServiceConfig& config,
    ui32 blockCount,
    ui32 channelCount,
    std::unique_ptr<NKikimr::TTabletStorageInfo> tabletInfo,
    TTestPartitionInfo partitionInfo = TTestPartitionInfo(),
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default,
    NProto::TDiagnosticsConfig diagnosticsConfig = {},
    NKikimr::TControlBoard* controlBoard = nullptr);

TString BuildRemoteHttpQuery(
    ui64 tabletId,
    const TVector<std::pair<TString, TString>>& keyValues,
    const TString& fragment = "");

NProto::TStorageServiceConfig DefaultConfig(ui32 flushBlobSizeThreshold = 4_KB);

std::unique_ptr<NActors::TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TStorageServiceConfig config = DefaultConfig(),
    ui32 blockCount = 1024,
    TMaybe<ui32> channelsCount = {},
    const TTestPartitionInfo& testPartitionInfo = TTestPartitionInfo(),
    NActors::IActorPtr volumeProxy = {},
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default,
    NProto::TDiagnosticsConfig diagnosticsConfig = {},
    NKikimr::TControlBoard* controlBoard = nullptr);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
