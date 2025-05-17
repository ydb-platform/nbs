#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ETransactionType
{
    InitSchema,
    LoadState,
    AddBlobs,
    AddGarbage,
    AddUnconfirmedBlobs,
    ConfirmBlobs,
    Cleanup,
    CollectGarbage,
    Compaction,
    CreateCheckpoint,
    DeleteCheckpoint,
    DeleteGarbage,
    DescribeBlocks,
    GetChangedBlocks,
    GetUsedBlocks,
    RebuildBlockCount,
    RebuildUsedBlocks,
    ReadBlocks,
    ScanDiskBatch,
    WriteFreshBlocks,
    ZeroBlocks,
    UpdateLogicalUsedBlocks,
    CheckIndex,
    DescribeRange,
    DescribeBlob,

    Total,
    None,
};

class TTransactionTimeTracker
{
public:
    enum class EStatus
    {
        Inflight,
        Finished,
    };

    struct TBucketInfo
    {
        ETransactionType TransactionType;
        TString Key;
        TString Description;
        TString Tooltip;
    };

private:
    struct TTimeHistogram: public THistogram<TRequestUsTimeBuckets>
    {
        TTimeHistogram()
            : THistogram<TRequestUsTimeBuckets>(
                  EHistogramCounterOption::ReportSingleCounter)
        {}
    };

    struct TKey
    {
        ETransactionType TransactionType = ETransactionType::None;
        EStatus Status = EStatus::Inflight;

        [[nodiscard]] TString GetHtmlPrefix() const;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    struct TEqual
    {
        bool operator()(const TKey& lhs, const TKey& rhs) const;
    };

    struct TTransactionInflight
    {
        ui64 StartTime = 0;
        ETransactionType TransactionType = ETransactionType::None;
    };

    THashMap<ui64, TTransactionInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash, TEqual> Histograms;

public:
    TTransactionTimeTracker();

    static TVector<TBucketInfo> GetTransactionBuckets();
    static TVector<TBucketInfo> GetTimeBuckets();

    void OnStarted(
        ETransactionType transactionType,
        ui64 transactionId,
        ui64 startTime);

    void OnFinished(ui64 transactionId, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
};

}   // namespace NCloud::NBlockStore::NStorage
