#pragma once

#include "compaction_map.h"
#include "request_info.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/diagnostics/trace_reader.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/interconnect.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/stream/output.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NMonitoringUtils {

////////////////////////////////////////////////////////////////////////////////

enum class EAlertLevel
{
    SUCCESS,
    INFO,
    WARNING,
    DANGER
};

void BuildNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    const TString& redirect,
    EAlertLevel alertLevel = EAlertLevel::SUCCESS);

void BuildTabletNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    ui64 tabletId,
    EAlertLevel alertLevel = EAlertLevel::SUCCESS);

void GenerateBlobviewJS(IOutputStream& out);
void GenerateActionsJS(IOutputStream& out);
void GeneratePartitionTabsJs(IOutputStream& out);

void BuildCreateCheckpointButton(IOutputStream& out, ui64 tabletId);

void BuildDeleteCheckpointButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& checkpointId);

void BuildAddGarbageButton(IOutputStream& out, ui64 tabletId);
void BuildCollectGarbageButton(IOutputStream& out, ui64 tabletId);
void BuildSetHardBarriers(IOutputStream& out, ui64 tabletId);
void BuildMenuButton(IOutputStream& out, const TString& menuItems);
void BuildPartitionTabs(IOutputStream& out);

void BuildReassignChannelsButton(
    IOutputStream& out,
    ui64 hiveTabletId,
    ui64 tabletId);

void BuildReassignChannelButton(
    IOutputStream& out,
    ui64 hiveTabletId,
    ui64 tabletId,
    ui32 channel);

void BuildForceCompactionButton(IOutputStream& out, ui64 tabletId);
void BuildForceCompactionButton(
    IOutputStream& out,
    ui64 tabletId,
    ui32 blockIndex);

void BuildForceCleanupButton(IOutputStream& out, ui64 tabletId);

void BuildRebuildMetadataButton(
    IOutputStream& out,
    ui64 tabletId,
    ui32 rangesPerBatch);

void BuildScanDiskButton(
    IOutputStream& out,
    ui64 tabletId,
    ui32 blobsPerBatch);

void BuildConfirmActionDialog(
    IOutputStream& out,
    const TString& id,
    const TString& title,
    const TString& message,
    const TString& onClickScript);

void DumpDefaultHeader(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    ui32 nodeId,
    const TDiagnosticsConfig& config);

void DumpDescribeHeader(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage);

void DumpCheckHeader(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage);

void DumpBlockIndex(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    ui32 blockIndex,
    ui64 commitId);

void DumpBlockIndex(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    ui32 blockIndex);

void DumpBlobId(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    const TPartialBlobId& blobId);

void DumpBlobOffset(IOutputStream& out, ui16 blobOffset);

void DumpCommitId(IOutputStream& out, ui64 commitId);

void DumpBlobs(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    const TVector<TPartialBlobId>& blobs);

void DumpStatsCounters(
    IOutputStream& out,
    const NProto::TIOCounters& counters);

void DumpPartitionStats(
    IOutputStream& out,
    const NProto::TPartitionConfig& config,
    const NProto::TPartitionStats& stats,
    ui32 freshBlocksCount);

void DumpPartitionCounters(
    IOutputStream& out,
    const NProto::TPartitionStats& stats);

void DumpPartitionConfig(
    IOutputStream& out,
    const NProto::TPartitionConfig& config);

void DumpCompactionMap(
    IOutputStream& out,
    const NKikimr::TTabletStorageInfo& storage,
    const TVector<TCompactionCounter>& items,
    const ui32 rangeSize);

void DumpMonitoringVolumeLink(
    IOutputStream& out,
    const TDiagnosticsConfig& config,
    const TString& diskId);

void DumpMonitoringPartitionLink(
    IOutputStream& out,
    const TDiagnosticsConfig& config);

void DumpBlockContent(IOutputStream& out, const TString& data);
void DumpDataHash(IOutputStream& out, const TString& data);

void DumpTabletNotReady(IOutputStream& out);

void BuildVolumeTabs(IOutputStream& out);
void DumpTraceLog(IOutputStream& out, const TVector<ITraceReaderPtr>& Readers);

void DumpLatency(IOutputStream& out, ui64 tabletId);

TCgiParameters GatherHttpParameters(const NActors::NMon::TEvRemoteHttpInfo& msg);
TCgiParameters GetHttpMethodParameters(const NActors::NMon::TEvRemoteHttpInfo& msg);
HTTP_METHOD GetHttpMethodType(const NActors::NMon::TEvRemoteHttpInfo& msg);

}   // namespace NCloud::NBlockStore::NStorage::NMonitoringUtils
