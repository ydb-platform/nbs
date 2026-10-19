#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>

#include <util/generic/string.h>

#include <memory>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

bool SelectMergedBlobCompression(
    const TStorageConfig& config,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    ui64 commitId,
    ui32 ordinal,
    bool background);

// Register once in the process counter tree, not in each disk counter tree.
void RegisterMergedBlobCompressionCounters(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters);

// Counts protocol rejection boundaries, including legacy consumers/providers.
void ReportMergedBlobCompatibilityRejection();

// Bounded per-blob codec/descriptor workspace; read query and scatter vectors
// are charged separately using their actual maximum lengths.
constexpr ui64 MergedBlobCompressionWorkspaceBytes = 1024 * 1024;

// Separate fixed pools reserve progress for foreground/background and reads/
// writes. No queued work or unbounded task spawning: writers fall back to raw,
// readers return a retryable rejection when their pool is full.
std::shared_ptr<void> TryAcquireMergedBlobBudget(
    bool background,
    bool read,
    ui64 bytes);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
