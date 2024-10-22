#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/storage/partition/model/block_mask.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <util/generic/bitmap.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/queue.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/stream/format.h>
#include <util/string/printf.h>

namespace {

using namespace NCloud::NBlockStore;

using NCloud::TCompressedBitmap;

////////////////////////////////////////////////////////////////////////////////

const ui32 BlockSize = 4_KB;
sig_atomic_t LoadStopped = false;

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    TVector<const char*> EvlogDumperArgv;
    ui32 WriteBlobThreshold;
    ui32 FlushThreshold;
    ui32 MaxBlobRangeSize;
    ui32 MaxBlobSize;
    ui32 CompactionThreshold;
    ui32 FilledBlockCount;
    ui32 FillLayers;
    ui32 Write2CompactionRatio;
    ui32 CompactionType;

    ui32 MaxReadIops;
    ui32 MaxReadBandwidth;
    ui32 MaxWriteIops;
    ui32 MaxWriteBandwidth;
    ui32 MaxGarbagePercentage;

    TOptions(int argc, const char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("evlog-dumper-params", "evlog dumper param string")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&EvlogDumperParamsStr);

        opts.AddLongOption("disk-id", "disk-id filter")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        opts.AddLongOption("write-blob-threshold", "WriteBlobThreshold")
            .RequiredArgument("INTEGER")
            .DefaultValue(1_MB)
            .StoreResult(&WriteBlobThreshold);

        opts.AddLongOption("flush-threshold", "FlushThreshold")
            .RequiredArgument("INTEGER")
            .DefaultValue(4_MB)
            .StoreResult(&FlushThreshold);

        opts.AddLongOption("max-blob-range-size", "MaxBlobRangeSize")
            .RequiredArgument("INTEGER")
            .DefaultValue(128_MB)
            .StoreResult(&MaxBlobRangeSize);

        opts.AddLongOption("max-blob-size", "MaxBlobSize")
            .RequiredArgument("INTEGER")
            .DefaultValue(4_MB)
            .StoreResult(&MaxBlobSize);

        opts.AddLongOption("compaction-threshold", "CompactionThreshold")
            .RequiredArgument("INTEGER")
            .DefaultValue(5)
            .StoreResult(&CompactionThreshold);

        opts.AddLongOption(
                "filled-block-count",
                "fill the specified number of disk blocks before starting log replay")
            .RequiredArgument("INTEGER")
            .DefaultValue(0)
            .StoreResult(&FilledBlockCount);

        opts.AddLongOption(
                "fill-layers",
                "fill the disk N times (works together with filled-block-count)")
            .RequiredArgument("INTEGER")
            .DefaultValue(1)
            .StoreResult(&FillLayers);

        opts.AddLongOption(
                "write-to-compaction-ratio",
                "compaction attempt will be made after every N writes")
            .RequiredArgument("INTEGER")
            .DefaultValue(10)
            .StoreResult(&Write2CompactionRatio);

        opts.AddLongOption(
                "compaction-type",
                "compaction policy id (0 == production-like)")
            .RequiredArgument("INTEGER")
            .DefaultValue(0)
            .StoreResult(&CompactionType);

        opts.AddLongOption(
                "max-read-iops",
                "max blobstorage read iops")
            .RequiredArgument("INTEGER")
            .DefaultValue(400)
            .StoreResult(&MaxReadIops);

        opts.AddLongOption(
                "max-read-bandwidth",
                "max blobstorage read bandwidth")
            .RequiredArgument("INTEGER")
            .DefaultValue(15_MB)
            .StoreResult(&MaxReadBandwidth);

        opts.AddLongOption(
                "max-write-iops",
                "max blobstorage write iops")
            .RequiredArgument("INTEGER")
            .DefaultValue(1000)
            .StoreResult(&MaxWriteIops);

        opts.AddLongOption(
                "max-write-bandwidth",
                "max blobstorage write bandwidth")
            .RequiredArgument("INTEGER")
            .DefaultValue(15_MB)
            .StoreResult(&MaxWriteBandwidth);

        opts.AddLongOption(
                "max-garbage-percentage",
                "max garbage percentage for non-default compaction policies")
            .RequiredArgument("INTEGER")
            .DefaultValue(10)
            .StoreResult(&MaxGarbagePercentage);

        TOptsParseResultException(&opts, argc, argv);

        EvlogDumperArgv.push_back("fake");

        TStringBuf sit(EvlogDumperParamsStr);
        TStringBuf arg;
        while (sit.NextTok(' ', arg)) {
            if (sit.size()) {
                const auto idx = EvlogDumperParamsStr.size() - sit.size() - 1;
                EvlogDumperParamsStr[idx] = 0;
            }
            EvlogDumperArgv.push_back(arg.data());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStat
{
    ui64 Count = 0;
    ui64 Bytes = 0;

    void Reg(ui64 blocks, ui32 count = 1)
    {
        Count += count;
        Bytes += blocks * BlockSize;
    }
};

struct TRWStat
{
    TStat Read;
    TStat Write;
};

struct TFullStat
{
    TRWStat FreshStat;
    TRWStat BlobStat;
    TRWStat UserStat;

    TStat FlushStat;
    TRWStat CompactionStat;

    TInstant PreLastEventTimestamp;
    TInstant LastEventTimestamp;
    TInstant PreRealTimestamp;
    TInstant RealTimestamp;

    double GarbagePercentage = 0;
    ui64 UsedBlockCount = 0;
    ui64 BlockCount = 0;
    ui64 MixedBlockCount = 0;
    ui64 MergedBlockCount = 0;
    float LastCompactionScore = 0;
    float LastGarbageCompactionScore = 0;
};

double Round(double x)
{
    return round(x * 10) / 10;
}

TString Bytes2String(double bytes)
{
    if (bytes >= 1_GB) {
        return TStringBuilder() << Prec(Round(bytes / 1_GB), PREC_AUTO) << " GiB";
    } else if (bytes >= 1_MB) {
        return TStringBuilder() << Prec(Round(bytes / 1_MB), PREC_AUTO) << " MiB";
    } else {
        return TStringBuilder() << Prec(Round(bytes / 1_KB), PREC_AUTO) << " KiB";
    }
}

////////////////////////////////////////////////////////////////////////////////

const auto CONSOLE_MAGENTA = "\033[95m";
const auto CONSOLE_RED = "\033[91m";
const auto CONSOLE_GREEN = "\033[92m";
const auto CONSOLE_YELLOW = "\033[93m";
const auto CONSOLE_ENDC = "\033[0m";
const auto WIPE = TString(20, ' ');

template <class T>
TString Indented(const T& t, ui32 w)
{
    auto s = ToString(t);
    if (s.Size() < w) {
        return TString(w - s.Size(), ' ') + s;
    }

    return s;
}

void ResetOutput(ui32 rows)
{
    Cout << static_cast<char>(27) << "[" << rows << "A";
}

void PrintStatHeader()
{
    Cout << CONSOLE_MAGENTA
        << "Metric"
        << TString(19, ' ') << "Count"
        << TString(15, ' ') << "Bytes"
        << TString(6, ' ') << "AvgRequestSize"
        << CONSOLE_ENDC << Endl;
}

void PrintStat(const TString& label, const TString& sublabel, const TStat& stat)
{
    const auto bpr = stat.Count ? stat.Bytes / stat.Count : 0;

    Cout << CONSOLE_YELLOW << label << CONSOLE_ENDC
        << CONSOLE_GREEN << sublabel << CONSOLE_ENDC
        << " " << Indented(stat.Count, 30 - label.size() - sublabel.size())
        << " " << Indented(Bytes2String(stat.Bytes), 20)
        << " " << Indented(Bytes2String(bpr), 20)
        << WIPE << Endl;
}

void PrintStat(const TString& label, const TRWStat& stat)
{
    PrintStat(label, "Read", stat.Read);
    PrintStat(label, "Write", stat.Write);
}

ui32 PrintStat(const TFullStat& stat)
{
    Cout << Endl;

    PrintStatHeader();

    const auto wampl =
        (stat.BlobStat.Write.Bytes + stat.FreshStat.Write.Bytes)
        / double(stat.UserStat.Write.Bytes);
    const auto rampl =
        (stat.BlobStat.Read.Bytes + stat.FreshStat.Read.Bytes)
        / double(stat.UserStat.Read.Bytes);

    PrintStat("Fresh", stat.FreshStat);
    PrintStat("Blob", stat.BlobStat);
    PrintStat("User", stat.UserStat);
    PrintStat("Flush", "BlobWrite", stat.FlushStat);
    PrintStat("Compaction", stat.CompactionStat);

    Cout << (stat.GarbagePercentage > 20 ? CONSOLE_RED : CONSOLE_GREEN) << "Garbage"
        << " " << Round(stat.GarbagePercentage) << "%" << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "Used"
        << " " << Bytes2String(stat.UsedBlockCount * BlockSize) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "Stored"
        << " " << Bytes2String(stat.BlockCount * BlockSize) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "Mixed"
        << " " << Bytes2String(stat.MixedBlockCount * BlockSize) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "Merged"
        << " " << Bytes2String(stat.MergedBlockCount * BlockSize) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "LastCompactionScore"
        << " " << stat.LastCompactionScore << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << "LastGarbageCompactionScore"
        << " " << stat.LastGarbageCompactionScore << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << (wampl > 3 ? CONSOLE_RED : CONSOLE_GREEN) << "Write Ampl"
        << " " << Round(wampl) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << (rampl > 3 ? CONSOLE_RED : CONSOLE_GREEN) << "Read Ampl"
        << " " << Round(rampl) << CONSOLE_ENDC
        << WIPE << Endl;

    const auto logicalPassed =
        stat.LastEventTimestamp - stat.PreLastEventTimestamp;
    const auto realPassed = stat.RealTimestamp - stat.PreRealTimestamp;
    const auto speed = double(logicalPassed.MilliSeconds())
        / realPassed.MilliSeconds();

    Cout << (speed > 1 ? CONSOLE_GREEN : CONSOLE_RED) << "Replay Speed"
        << " x" << Round(speed) << CONSOLE_ENDC
        << WIPE << Endl;

    Cout << stat.LastEventTimestamp.ToString() << Endl;

    return 22;
}

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlobInfo
{
    TVector<ui64> Blocks;
};

struct TFreshIndex
{
    const ui32 MaxBlobSize;
    const ui32 MaxBlobRangeSize;
    THashSet<ui64> Blocks;

    TFreshIndex(ui32 maxBlobSize, ui32 maxBlobRangeSize)
        : MaxBlobSize(maxBlobSize)
        , MaxBlobRangeSize(maxBlobRangeSize)
    {
    }

    void Write(ui64 index, ui32 blocks)
    {
        for (ui32 i = 0; i < blocks; ++i) {
            Blocks.insert(index + i);
        }
    }

    ui32 Read(ui64 index, ui32 blocks) const
    {
        ui32 c = 0;
        for (ui32 i = 0; i < blocks; ++i) {
            c += Blocks.contains(index + i);
        }
        return c;
    }

    TVector<TMixedBlobInfo> Flush()
    {
        Y_ABORT_UNLESS(Blocks.size());

        TVector<ui64> blocks(Blocks.begin(), Blocks.end());
        Blocks.clear();

        Sort(blocks);

        TVector<TMixedBlobInfo> blobs(1);
        const auto maxRangeSize = MaxBlobRangeSize / BlockSize;
        const auto maxBlocks = MaxBlobSize / BlockSize;
        for (auto block: blocks) {
            auto* blob = &blobs.back();
            if (blob->Blocks.size()
                    && block - blob->Blocks.front() >= maxRangeSize
                    || blob->Blocks.size() == maxBlocks)
            {
                blob = &blobs.emplace_back();
            }

            blob->Blocks.push_back(block);
        }

        for (const auto& blob: blobs) {
            Y_ABORT_UNLESS(blob.Blocks.size());
        }

        return blobs;
    }

    ui64 GetBlockCount() const
    {
        return Blocks.size();
    }

    ui64 Size() const
    {
        return Blocks.size() * BlockSize;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCompactionRange: TAtomicRefCount<TCompactionRange>
{
    ui64 Index;
    ui32 Blobs = 0;
    ui32 Bytes = 0;
    bool Deleted = false;

    ui32 ReadCount = 0;
    ui32 ReadBytes = 0;
    ui32 ReadBlobCount = 0;

    TCompactionRange(ui64 index)
        : Index(index)
    {
    }

    void Read(ui32 bytes, ui32 blobCount)
    {
        ++ReadCount;
        ReadBytes += bytes;
        ReadBlobCount += blobCount;
    }
};

using TCompactionRangePtr = TIntrusivePtr<TCompactionRange>;

double RequestCost(
    ui32 maxIops,
    ui32 maxBandwidth,
    double bytes,
    double count)
{
    return count / maxIops + bytes / maxBandwidth;
}

////////////////////////////////////////////////////////////////////////////////

struct ICompactionScoreCalculator
    : TThrRefBase
{
    virtual double Calculate(const TCompactionRange& range) const = 0;
};

using ICompactionScoreCalculatorPtr = TIntrusivePtr<ICompactionScoreCalculator>;

struct TSimpleCompactionScoreCalculator final
    : ICompactionScoreCalculator
{
    double Calculate(const TCompactionRange& range) const override
    {
        return range.Blobs;
    }
};

struct TDynamicCompactionScoreCalculator final
    : ICompactionScoreCalculator
{
    const ui32 MaxBlobSize;
    const ui32 MaxReadIops;
    const ui32 MaxReadBandwidth;
    const ui32 MaxWriteIops;
    const ui32 MaxWriteBandwidth;

    TDynamicCompactionScoreCalculator(
            ui32 maxBlobSize,
            ui32 maxReadIops,
            ui32 maxReadBandwidth,
            ui32 maxWriteIops,
            ui32 maxWriteBandwidth)
        : MaxBlobSize(maxBlobSize)
        , MaxReadIops(maxReadIops)
        , MaxReadBandwidth(maxReadBandwidth)
        , MaxWriteIops(maxWriteIops)
        , MaxWriteBandwidth(maxWriteBandwidth)
    {
    }

    double Calculate(const TCompactionRange& range) const override
    {
        auto readCost = RequestCost(
            MaxReadIops,
            MaxReadBandwidth,
            range.ReadBytes,
            range.ReadBlobCount
        );

        auto compactedReadCost = RequestCost(
            MaxReadIops,
            MaxReadBandwidth,
            range.ReadBytes,
            range.ReadCount
        );

        auto compactionCost = RequestCost(
            MaxReadIops,
            MaxReadBandwidth,
            Min(range.Bytes, MaxBlobSize),
            range.Blobs
        ) + RequestCost(
            MaxWriteIops,
            MaxWriteBandwidth,
            Min(range.Bytes, MaxBlobSize),
            1
        );

        return readCost - compactedReadCost - compactionCost - 1e-10;
    }
};

struct TCompactionRangeComparator
{
    ICompactionScoreCalculatorPtr Calc;

    TCompactionRangeComparator(ICompactionScoreCalculatorPtr calc)
        : Calc(std::move(calc))
    {
    }

    bool operator()(
        const TCompactionRangePtr& l,
        const TCompactionRangePtr& r) const
    {
        return Calc->Calculate(*l) < Calc->Calculate(*r);
    }
};

struct TCompareByByteCount
{
    bool operator()(
        const TCompactionRangePtr& l,
        const TCompactionRangePtr& r) const
    {
        return l->Bytes < r->Bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockLocation
{
    ui64 BlobId = 0;
    ui16 BlobOffset = 0;

    TBlockLocation() = default;

    TBlockLocation(ui64 blobId, ui16 blobOffset)
        : BlobId(blobId)
        , BlobOffset(blobOffset)
    {
    }
};

struct TMergedBlobInfo
{
    ui64 BlobId = 0;
    ui64 FirstIndex = 0;
};

using NStorage::NPartition::IsBlockMaskFull;
using TBlockMask = NStorage::NPartition::TBlockMask;

struct TBlobIndex
{
    const ICompactionScoreCalculatorPtr Calc;

    // Options
    const ui32 MaxBlobSize;
    const ui32 CompactionThreshold;

    // RangeIndex -> Score
    TCompactionRangeComparator CompactionRangeComp;
    TDeque<TCompactionRangePtr> CompactionMap;
    // TODO: don't use heap! just keep a queue of accessed ranges
    // check recently accessed ranges upon compaction attempt
    // then add the ranges whose scores are higher than threshold
    // to this queue
    using TCompactionRanges = TPriorityQueue<
        TCompactionRangePtr,
        TVector<TCompactionRangePtr>,
        TCompactionRangeComparator
    >;
    using TCompactionRangesByByteCount = TPriorityQueue<
        TCompactionRangePtr,
        TVector<TCompactionRangePtr>,
        TCompareByByteCount
    >;
    mutable TCompactionRanges CompactionRanges;
    mutable TCompactionRangesByByteCount CompactionRangesByByteCount;

    struct TBlobInfo
    {
        TBlockMask Mask;
        ui32 BlockCount = 0;
    };
    // BlobId -> info
    THashMap<ui64, TBlobInfo> Blobs;
    // BlockIndex -> BlobIds
    THashMap<ui64, TBlockLocation> MixedIndex;
    // LastBlockIndex -> BlobInfos
    TMap<ui64, TVector<TMergedBlobInfo>> MergedIndex;

    // Incremented for every new blob
    ui64 LastBlobId = 0;
    ui64 MixedBlockCount = 0;
    ui64 MergedBlockCount = 0;

    ui64 MaxBlockIndex = 0;

    TBlobIndex(
            ICompactionScoreCalculatorPtr calc,
            ui32 maxBlobSize,
            ui32 compactionThreshold)
        : Calc(std::move(calc))
        , MaxBlobSize(maxBlobSize)
        , CompactionThreshold(compactionThreshold)
        , CompactionRangeComp(Calc)
        , CompactionRanges(CompactionRangeComp)
    {
    }

    struct TBlobBlocks
    {
        bool Mixed;
        ui64 BlobId;
        TVector<ui64> Blocks;
        TVector<ui16> Offsets;

        TBlobBlocks(bool mixed, ui64 blobId)
            : Mixed(mixed)
            , BlobId(blobId)
        {
        }
    };

    //
    // Helpers
    //

    TVector<TBlobBlocks> FindBlocks(ui64 index, ui32 blocks) const
    {
        TVector<TBlobBlocks> bblocks;

        {
            THashMap<ui64, TVector<std::pair<ui64, ui16>>> mixedBlob2Blocks;
            for (ui32 i = 0; i < blocks; ++i) {
                if (auto* p = MixedIndex.FindPtr(index + i)) {
                    mixedBlob2Blocks[p->BlobId].push_back(
                        {index + i, p->BlobOffset}
                    );
                }
            }

            for (auto& x: mixedBlob2Blocks) {
                auto& blobData = bblocks.emplace_back(true, x.first);
                blobData.Blocks.reserve(x.second.size());
                blobData.Offsets.reserve(x.second.size());
                for (const auto& y: x.second) {
                    blobData.Blocks.push_back(y.first);
                    blobData.Offsets.push_back(y.second);
                }
            }
        }

        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        const auto range = TBlockRange64::WithLength(index, blocks);
        auto it = MergedIndex.lower_bound(index);

        while (it != MergedIndex.end()) {
            const auto maxRange = TBlockRange64::MakeClosedInterval(
                it->first + 1 >= maxBlocksInBlob
                    ? it->first - maxBlocksInBlob + 1
                    : 0,
                it->first
            );

            if (!range.Overlaps(maxRange)) {
                break;
            }

            for (const auto& blobInfo: it->second) {
                const auto blobRange = TBlockRange64::MakeClosedInterval(
                    blobInfo.FirstIndex,
                    it->first);
                if (range.Overlaps(blobRange)) {
                    const auto subRange = range.Intersect(blobRange);
                    const auto& blockMask = Blobs.at(blobInfo.BlobId).Mask;

                    auto& blobData =
                        bblocks.emplace_back(false, blobInfo.BlobId);
                    blobData.Blocks.reserve(subRange.Size());

                    for (ui64 b = subRange.Start; b <= subRange.End; ++b) {
                        if (!blockMask.Test(b - blobRange.Start)) {
                            blobData.Blocks.push_back(b);
                            blobData.Offsets.push_back(b - blobRange.Start);
                        }
                    }

                    if (blobData.Blocks.empty()) {
                        bblocks.pop_back();
                    }
                }
            }

            ++it;
        }

        return bblocks;
    }

    void DeleteMergedBlob(const ui64 blobId, const TVector<ui64>& blocks)
    {
        auto it = MergedIndex.lower_bound(blocks.front());
        bool deleted = false;
        while (it != MergedIndex.end()) {
            ui32 i = 0;
            while (i < it->second.size()) {
                if (it->second[i].BlobId == blobId) {
                    break;
                }

                ++i;
            }

            if (i < it->second.size()) {
                if (i != it->second.size() - 1) {
                    DoSwap(it->second[i], it->second.back());
                    it->second.resize(it->second.size() - 1);
                } else {
                    if (it->second.size() == 1) {
                        MergedIndex.erase(it);
                    } else {
                        it->second.pop_back();
                    }
                }

                deleted = true;
                break;
            }

            ++it;
        }

        Y_ABORT_UNLESS(deleted);
    }

    /*
    THashSet<ui64> XXX = {
    };
    */

    ui32 DeleteBlocks(
        ui64 index,
        ui32 blocks,
        TVector<ui64>* deleted = nullptr)
    {
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;

        const auto bblocks = FindBlocks(index, blocks);
        for (const auto& blobBlocks: bblocks) {
            auto& blobInfo = Blobs.at(blobBlocks.BlobId);
            for (ui16 i = 0; i < blobBlocks.Blocks.size(); ++i) {
                /*
                if (XXX.contains(blobBlocks.Blocks[i])) {
                    Cerr << "marking the block " << blobBlocks.Blocks[i] << " deleted in blob " << blobBlocks.BlobId << ", offset=" << blobBlocks.Offsets[i] << Endl;
                }
                */

                blobInfo.Mask.Set(blobBlocks.Offsets[i]);

                if (deleted) {
                    deleted->push_back(blobBlocks.Blocks[i]);
                }

                if (blobBlocks.Mixed) {
                    MixedIndex.erase(blobBlocks.Blocks[i]);
                }
            }

            const bool full = IsBlockMaskFull(blobInfo.Mask, maxBlocksInBlob);

            if (full) {
                if (blobBlocks.Mixed) {
                    MixedBlockCount -= blobInfo.BlockCount;
                } else {
                    DeleteMergedBlob(blobBlocks.BlobId, blobBlocks.Blocks);
                    MergedBlockCount -= blobInfo.BlockCount;
                }

                Blobs.erase(blobBlocks.BlobId);
            }
        }

        return bblocks.size();
    }

    //
    // Blobs
    //

    void Write(const TVector<ui64>& blocks)
    {
        Y_ABORT_UNLESS(blocks.size());
        Y_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        ui64 prevRangeIndex = Max<ui64>();
        ui32 blockCount = 0;

        const auto onRange = [&] () {
            Y_ABORT_UNLESS(prevRangeIndex != Max<ui64>());

            UpdateCompactionRange(
                prevRangeIndex,
                ECompactionRangeActionType::Write,
                BlockSize * blockCount,
                1
            );
        };

        for (const auto b: blocks) {
            DeleteBlocks(b, 1);

            const auto rangeIndex = b / maxBlocksInBlob;
            if (rangeIndex != prevRangeIndex) {
                if (prevRangeIndex != Max<ui64>()) {
                    onRange();
                }

                prevRangeIndex = rangeIndex;
                blockCount = 0;
            }

            MaxBlockIndex = Max(b, MaxBlockIndex);

            ++blockCount;
        }

        onRange();

        ++LastBlobId;

        ui16 i = 0;
        for (const auto b: blocks) {
            MixedIndex[b] = {LastBlobId, i};
            ++i;
        }

        Blobs[LastBlobId].BlockCount = blocks.size();
        MixedBlockCount += blocks.size();
    }

    void Write(ui64 index, ui32 blocks)
    {
        Y_ABORT_UNLESS(blocks);

        const auto blockRange = TBlockRange64::WithLength(index, blocks);
        const auto ranges = ToCompactionRanges(index, blocks);
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        for (ui32 i = ranges.FirstRange; i <= ranges.LastRange; ++i) {
            const auto compactionRange = TBlockRange64::WithLength(
                i * maxBlocksInBlob,
                maxBlocksInBlob
            );

            UpdateCompactionRange(
                i,
                ECompactionRangeActionType::Write,
                BlockSize * blockRange.Intersect(compactionRange).Size(),
                1
            );
        }

        // XXX
        /*
        for (const auto b: XXX) {
            if (blockRange.Contains(b)) {
                Cerr << "writing the block " << b << " via " << index << " - " << (index + blocks - 1) << ", blob id=" << (LastBlobId + 1) << ", offset=" << (b - index) << Endl;
            }
        }
        */


        DeleteBlocks(index, blocks);

        ++LastBlobId;
        MergedIndex[index + blocks - 1].push_back(
            {LastBlobId, index}
        );
        Blobs[LastBlobId].BlockCount = blocks;
        MergedBlockCount += blocks;

        MaxBlockIndex = Max(index + blocks - 1, MaxBlockIndex);
    }

    // returns (blockCount, blobCount)
    std::pair<ui32, ui32> Read(ui64 index, ui32 blocks)
    {
        auto bblocks = FindBlocks(index, blocks);
        ui32 blockCount = 0;
        for (const auto& x: bblocks) {
            blockCount += x.Blocks.size();
        }

        const auto blockRange = TBlockRange64::WithLength(index, blocks);
        const auto ranges = ToCompactionRanges(index, blocks);
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        for (ui32 i = ranges.FirstRange; i <= ranges.LastRange; ++i) {
            const auto compactionRange = TBlockRange64::WithLength(
                i * maxBlocksInBlob,
                maxBlocksInBlob
            );

            UpdateCompactionRange(
                i,
                ECompactionRangeActionType::Read,
                BlockSize * blockRange.Intersect(compactionRange).Size(),
                bblocks.size()
            );
        }

        return std::make_pair(blockCount, bblocks.size());
    }

    //
    // Compaction
    //

    struct TCompactionRangesRange
    {
        ui64 FirstRange;
        ui64 LastRange;

        TCompactionRangesRange(ui64 firstRange, ui64 lastRange)
            : FirstRange(firstRange)
            , LastRange(lastRange)
        {
        }
    };

    TCompactionRangesRange ToCompactionRanges(ui64 index, ui32 blocks) const
    {
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        return {
            index / maxBlocksInBlob,
            (index + blocks - 1) / maxBlocksInBlob
        };
    }

    enum class ECompactionRangeActionType
    {
        Reset,
        Write,
        Read,
    };

    void UpdateCompactionRange(
        ui64 rangeIndex,
        ECompactionRangeActionType action,
        ui32 bytes,
        ui32 blobs)
    {
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;
        if (rangeIndex >= CompactionMap.size()) {
            CompactionMap.resize(rangeIndex + 1);
        }

        auto range = MakeIntrusive<TCompactionRange>(
            rangeIndex * maxBlocksInBlob
        );

        if (CompactionMap[rangeIndex]) {
            if (action != ECompactionRangeActionType::Reset) {
                *range = *CompactionMap[rangeIndex];
            }
            CompactionMap[rangeIndex]->Deleted = true;
        }

        switch (action) {
            case ECompactionRangeActionType::Reset: {
                break;
            }

            case ECompactionRangeActionType::Write: {
                range->Blobs += blobs;
                range->Bytes += bytes;
                break;
            }

            case ECompactionRangeActionType::Read: {
                range->Read(bytes, blobs);
                break;
            }
        }

        const auto score = Calc->Calculate(*range);
        Cdbg << "rangeIndex=" << rangeIndex
            << "\tblobs=" << range->Blobs
            << "\tbytes=" << range->Bytes
            << "\treadCount=" << range->ReadCount
            << "\treadBytes=" << range->ReadBytes
            << "\treadBlobs=" << range->ReadBlobCount
            << "\tscore=" << score << Endl;

        if (score >= CompactionThreshold) {
            CompactionRanges.push(range);
        }
        if (range->Bytes > MaxBlobSize) {
            CompactionRangesByByteCount.push(range);
        }
        CompactionMap[rangeIndex] = range;
    }

    TCompactionRange TopRange() const
    {
        while (CompactionRanges.size()) {
            auto top = CompactionRanges.top();
            CompactionRanges.pop();

            if (!top->Deleted) {
                return *top;
            }
        }

        return {0};
    }

    TCompactionRange TopRangeByByteCount() const
    {
        while (CompactionRangesByByteCount.size()) {
            auto top = CompactionRangesByByteCount.top();
            CompactionRangesByByteCount.pop();

            if (!top->Deleted) {
                return *top;
            }
        }

        return {0};
    }

    std::pair<ui32, TVector<ui64>> Compact(ui64 index)
    {
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;

        TVector<ui64> deleted;
        auto blobCount = DeleteBlocks(index, maxBlocksInBlob, &deleted);
        UpdateCompactionRange(
            index / maxBlocksInBlob,
            ECompactionRangeActionType::Reset,
            deleted.size() * BlockSize,
            1
        );

        return {blobCount, std::move(deleted)};
    }

    //
    // Stats
    //

    ui64 GetMixedBlockCount() const
    {
        return MixedBlockCount;
    }

    ui64 GetMergedBlockCount() const
    {
        return MergedBlockCount;
    }

    ui64 GetBlockCount() const
    {
        return MixedBlockCount + MergedBlockCount;
    }

    //
    // Validation
    //

    void ValidateGarbage(TLog& log)
    {
        const auto maxBlocksInBlob = MaxBlobSize / BlockSize;

        ui64 liveBlocks = 0;
        ui64 storedBlocks = 0;
        for (const auto& x: Blobs) {
            liveBlocks += x.second.BlockCount - x.second.Mask.Count();
            storedBlocks += x.second.BlockCount;
            Y_ABORT_UNLESS(!IsBlockMaskFull(x.second.Mask, maxBlocksInBlob));
        }

        log << "Live " << Bytes2String(liveBlocks * BlockSize) << "\n";
        log << "Stored " << Bytes2String(storedBlocks * BlockSize) << "\n";

        TCompressedBitmap blocks(MaxBlockIndex + 1);
        for (const auto& x: MixedIndex) {
            const auto& mask = Blobs.at(x.second.BlobId).Mask;
            if (mask.Test(x.second.BlobOffset)) {
                continue;
            }

            if (blocks.Test(x.first)) {
                log << "duplicate block " << x.first << "\n";
            } else {
                blocks.Set(x.first, x.first + 1);
            }
        }

        for (const auto& x: MergedIndex) {
            for (const auto& y: x.second) {
                const auto& mask = Blobs.at(y.BlobId).Mask;
                for (auto b = y.FirstIndex; b <= x.first; ++b) {
                    if (mask.Test(b - y.FirstIndex)) {
                        continue;
                    }

                    /*
                    if (XXX.contains(b)) {
                        Cerr << "the block " << b << " found in blob " << y.BlobId << " at offset " << b - y.FirstIndex << Endl;
                    }
                    */

                    if (blocks.Test(b)) {
                        log << "duplicate block " << b << "\n";
                    }

                    blocks.Set(b, b + 1);
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICompactionScoreCalculatorPtr BuildCompactionRangeScoreCalculator(
    const TOptions& options)
{
    if (options.CompactionType == 1) {
        return new TDynamicCompactionScoreCalculator(
            options.MaxBlobSize,
            options.MaxReadIops,
            options.MaxReadBandwidth,
            options.MaxWriteIops,
            options.MaxWriteBandwidth
        );
    }

    return new TSimpleCompactionScoreCalculator();
}

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;
    TLog& Log;
    TFullStat Stat;
    TFreshIndex FreshIndex;
    TBlobIndex BlobIndex;
    TCompressedBitmap UsedBlocks;
    ui32 LastRowsPrinted = 0;

public:
    TEventProcessor(const TOptions& options, TLog& log)
        : Options(options)
        , Log(log)
        , FreshIndex(options.MaxBlobSize, options.MaxBlobRangeSize)
        , BlobIndex(
            BuildCompactionRangeScoreCalculator(options),
            options.MaxBlobSize,
            options.CompactionType != 1 ? options.CompactionThreshold : 0
        )
        , UsedBlocks(options.FilledBlockCount)
    {
        if (options.FilledBlockCount) {
            for (ui32 i = 0; i < options.FillLayers; ++i) {
                WriteBlocks(0, options.FilledBlockCount);
                Stat = TFullStat();
            }
        }
    }

public:
    const TFullStat& GetStat() const
    {
        return Stat;
    }

    void ValidateGarbage()
    {
        BlobIndex.ValidateGarbage(Log);
    }

protected:
    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message && message->GetDiskId() == Options.DiskId) {
            Stat.LastEventTimestamp = TInstant::MicroSeconds(ev->Timestamp);
            Stat.RealTimestamp = Now();

            struct TReq
            {
                EBlockStoreRequest Type;
                TInstant Ts;
                ui64 RelativeFirstBlockIndex;
                ui64 RelativeLastBlockIndex;

                bool operator<(const TReq& rhs) const
                {
                    return Ts < rhs.Ts;
                }
            };

            TVector<const NProto::TProfileLogRequestInfo*> reqs;
            reqs.reserve(message->RequestsSize());
            for (const auto& r: message->GetRequests()) {
                reqs.push_back(&r);
            }

            Sort(
                reqs.begin(),
                reqs.end(),
                [] (const NProto::TProfileLogRequestInfo* l,
                    const NProto::TProfileLogRequestInfo* r)
                {
                    // XXX not taking ts overflows into account
                    // but this should not seriously affect the results
                    return l->GetTimestampMcs() < r->GetTimestampMcs();
                }
            );

            for (const auto* req: reqs) {
                Process(*req);
            }
        }
    }

private:
    void Process(const NProto::TProfileLogRequestInfo& req)
    {
        bool keepRunning = true;

        if (LoadStopped) {
            FlushIfNeeded();
            keepRunning = CompactIfNeeded();
        } else {
            const auto t = static_cast<EBlockStoreRequest>(req.GetRequestType());

            if (t == EBlockStoreRequest::WriteBlocks) {
                WriteBlocks(req.GetBlockIndex(), req.GetBlockCount());

                FlushIfNeeded();
                if (Stat.UserStat.Write.Count % Options.Write2CompactionRatio == 0) {
                    CompactIfNeeded();
                }
            } else if (t == EBlockStoreRequest::ReadBlocks) {
                ReadBlocks(req.GetBlockIndex(), req.GetBlockCount());
            } else {
                // disregard ZeroBlocks
            }
        }

        const auto d = TDuration::MilliSeconds(10);
        if (Stat.RealTimestamp > Stat.PreRealTimestamp + d) {
            ResetOutput(LastRowsPrinted);
            Stat.UsedBlockCount = UsedBlocks.Count();
            Stat.BlockCount = FreshIndex.GetBlockCount() + BlobIndex.GetBlockCount();
            Stat.MixedBlockCount = BlobIndex.GetMixedBlockCount();
            Stat.MergedBlockCount = BlobIndex.GetMergedBlockCount();
            LastRowsPrinted = PrintStat(Stat);
            Stat.PreLastEventTimestamp = Stat.LastEventTimestamp;
            Stat.PreRealTimestamp = Stat.RealTimestamp;
        }

        if (!keepRunning) {
            PrintStat(Stat);
            exit(0);
        }
    }

    void WriteBlocks(const ui64 index, const ui32 blocks)
    {
        Log << "write\t" << index << "\t" << blocks << "\n";

        if (ui64(blocks) * BlockSize < Options.WriteBlobThreshold) {
            WriteFresh(index, blocks);
        } else {
            auto rem = blocks;
            auto i = index;
            while (rem) {
                const auto blobBlocks =
                    Min(rem, Options.MaxBlobSize / BlockSize);
                WriteBlob(i, blobBlocks);
                rem -= blobBlocks;
                i += blobBlocks;
            }
        }

        if (index < Options.FilledBlockCount) {
            UsedBlocks.Set(
                index,
                Min(
                    index + blocks,
                    static_cast<ui64>(Options.FilledBlockCount)
                )
            );
        }
        Stat.UserStat.Write.Reg(blocks);
    }

    void ReadBlocks(const ui64 index, const ui32 blocks)
    {
        Log << "read\t" << index << "\t" << blocks << "\n";

        ReadFresh(index, blocks);
        ReadBlobs(index, blocks);

        Stat.UserStat.Read.Reg(blocks);
    }

    void WriteFresh(const ui64 index, const ui32 blocks)
    {
        Log << "fresh write\t" << index << "\t" << blocks << "\n";

        FreshIndex.Write(index, blocks);
        Stat.FreshStat.Write.Reg(blocks);
    }

    void ReadFresh(const ui64 index, const ui32 blocks)
    {
        const auto count = FreshIndex.Read(index, blocks);

        if (count) {
            Log << "fresh read\t" << index << "\t" << blocks
                << "\t" << count << "\n";

            Stat.FreshStat.Read.Reg(count, count);
        }
    }

    void WriteBlob(const TVector<ui64>& blocks)
    {
        Log << "mixed write\t" << blocks.size()
            << "\t" << blocks.front()
            << "\t" << blocks.back()
            << "\n";
        Y_ABORT_UNLESS(blocks.size() <= Options.MaxBlobSize / BlockSize);

        BlobIndex.Write(blocks);
        Stat.BlobStat.Write.Reg(blocks.size());
    }

    void WriteBlob(const ui64 index, const ui32 blocks)
    {
        Log << "merged write\t" << index << "\t" << blocks << "\n";
        Y_ABORT_UNLESS(blocks <= Options.MaxBlobSize / BlockSize);

        BlobIndex.Write(index, blocks);
        Stat.BlobStat.Write.Reg(blocks);
    }

    void ReadBlobs(const ui64 index, const ui32 blocks)
    {
        ui32 blockCount, blobCount;
        std::tie(blockCount, blobCount) = BlobIndex.Read(
            index,
            blocks
        );
        Log << "blob read\t" << index << "\t" << blocks
            << "\t" << blockCount << "\t" << blobCount << "\n";

        Stat.BlobStat.Read.Reg(blockCount, blobCount);
    }

    void FlushIfNeeded()
    {
        if (FreshIndex.Size() >= Options.FlushThreshold) {
            auto blobs = FreshIndex.Flush();
            for (const auto& blob: blobs) {
                Log << "flush blob\t" << blob.Blocks.size() << "\n";

                WriteBlob(blob.Blocks);
                Stat.FlushStat.Reg(blob.Blocks.size(), 1);
            }

            CompactIfNeeded();
        }
    }

    void DoCompact(const TCompactionRange& topRange)
    {
        ui32 blobCount;
        TVector<ui64> blocks;
        std::tie(blobCount, blocks) = BlobIndex.Compact(topRange.Index);
        Log << "compact range\t" << topRange.Index
            << "\t" << topRange.Blobs
            << "\t" << topRange.Bytes
            << "\t" << blocks.size() << "\n";

        WriteBlob(topRange.Index, Options.MaxBlobSize / BlockSize);
        Stat.CompactionStat.Write.Reg(Options.MaxBlobSize / BlockSize, 1);

        Stat.BlobStat.Read.Reg(blocks.size(), blobCount);
        Stat.CompactionStat.Read.Reg(blocks.size(), blobCount);
    }

    bool CompactIfNeeded()
    {
        const double garbageBlocks = BlobIndex.GetBlockCount()
            + FreshIndex.GetBlockCount() - UsedBlocks.Count();
        const auto garbagePercentage =
            garbageBlocks * 100 / UsedBlocks.Count();
        Stat.GarbagePercentage = garbagePercentage;

        auto topRange = BlobIndex.TopRange();
        if (topRange.Blobs) {
            Stat.LastCompactionScore = BlobIndex.Calc->Calculate(topRange);
        } else if (UsedBlocks.Count() && Options.CompactionType) {
            if (garbagePercentage > Options.MaxGarbagePercentage) {
                topRange = BlobIndex.TopRangeByByteCount();
                Stat.LastGarbageCompactionScore = topRange.Bytes;
            }
        }

        if (topRange.Blobs) {
            DoCompact(topRange);

            return true;
        }

        return false;
    }
};

void ProcessAsyncSignal(int signum)
{
    if (signum == SIGHUP) {
        LoadStopped = true;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    SetAsyncSignalHandler(SIGHUP, ProcessAsyncSignal);

    TOptions options(argc, argv);
    TLog log(THolder(new TStreamLogBackend(&Cerr)));
    TEventProcessor processor(options, log);

    auto code = IterateEventLog(
        NEvClass::Factory(),
        &processor,
        options.EvlogDumperArgv.size(),
        options.EvlogDumperArgv.begin()
    );

    PrintStat(processor.GetStat());
    processor.ValidateGarbage();

    return code;
}
