#include <cloud/filestore/libs/storage/tablet/tablet.h>

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>

#include <memory>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4096;
constexpr ui64 FileSize = 1_MB;
constexpr ui32 BlockCount = FileSize / BlockSize;
constexpr ui64 MixedBlocksOffloadedRangesCapacity = 1000000;

// Maximum number of overlapping overwrite layers per range in the
// multi-overwrite mixed file.
constexpr ui32 MaxOverwrites = 4;

// Write size used to build the fresh file. It must stay below
// WriteBlobThreshold (128KiB by default) so that the data lands in the fresh
// blocks table instead of going down the blob write path.
constexpr ui64 FreshWriteSize = 64_KB;

// The read ahead file is separate from the 1MiB files above: read ahead only
// does anything across *consecutive* requests, and with a 1MiB file the very
// first window would already cover the whole file, so every later request
// would be a cache hit and nothing would ever be described again.
constexpr ui64 ReadAheadFileSize = 16_MB;

// ReadAheadRangeSize == 0 leaves the read ahead cache disabled, which is what
// the non-read-ahead benchmarks use.
//
// Read ahead is deliberately NOT enabled in the shared setup:
// IsCloseToSequential accepts a run of identical 1MiB describes as sequential
// (their spanned length is exactly 1MiB), so any RangeSize above 1MiB would
// make the existing 1MiB benchmarks start populating and then hitting the read
// ahead cache, silently changing what they measure. The read ahead benchmarks
// get their own tablets.
template <ui32 ReadAheadRangeSize>
struct TTabletSetupT
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> TabletClient;

    // Each file is created lazily, the first time the corresponding benchmark
    // runs, via an Ensure*Created method. The benchmark framework's untimed
    // func(1) warmup performs that one-time creation, so the construction cost
    // stays out of the measured per-iteration cycles and out of the shared
    // constructor. A handle is 0 until its file has been created, which also
    // serves as the idempotency guard.

    // A file backed by a single large blob, i.e. its block list is fully
    // |merged| - its TBlockList's contain a small number of long block ranges.
    ui64 MergedFileHandle = 0;

    // A file whose data has been fragmented and compacted so that its block
    // list contains a lot of |mixed| (non-contiguous) ranges. Its TBlockList's
    // contain many separate block ranges.
    ui64 MixedFileHandle = 0;

    // A file that starts from the same compacted mixed base as MixedFileHandle,
    // then has several (up to MaxOverwrites) overlapping overwrite blobs layered
    // on top without compacting them away. Reading it visits each overwritten
    // block once per overlapping blob. The node id is kept to resolve the
    // file's range ids.
    ui64 OverwriteFileHandle = 0;
    ui64 OverwriteNodeId = 0;

    // A file whose blocks are all still in the fresh blocks table: it is
    // written in sub-WriteBlobThreshold chunks and never flushed, so nothing
    // is backed by a blob. Describing it has to ship every block's content
    // inline in the response, unlike blob-backed files which are described by
    // reference.
    ui64 FreshFileHandle = 0;

    // A ReadAheadFileSize long blob-backed file used only by the read ahead
    // benchmarks.
    ui64 ReadAheadFileHandle = 0;

    TTabletSetupT()
        : Env(TTestEnvConfig{
            // Turn off logging in order to reduce performance overhead
            .LogPriority_NFS = NActors::NLog::PRI_ALERT,
            .LogPriority_KiKiMR = NActors::NLog::PRI_ALERT,
            .LogPriority_Others = NActors::NLog::PRI_ALERT})
    {
        NCloud::NFileStore::NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetMixedBlocksOffloadedRangesCapacity(
            MixedBlocksOffloadedRangesCapacity);

        // Disable *automatic* background blob-index operations.
        //
        // The tablet runs background operations after user operations and
        // on a timer, it triggers background compaction / cleanup / garbage
        // collection off tablet-wide thresholds. All files here share a single
        // tablet, so the fragmented mixed_multi_overwrite file keeps those
        // averages over the thresholds and background work fires during the
        // event dispatch of *every* DescribeData - including reads of the
        // merged file, spoiling every benchmark results.
        // It would also silently compact the overlapping blobs away, so the
        // multi-overwrite case would stop measuring what it is supposed to.
        //
        // Raising the thresholds keeps automatic operations from ever firing,
        // so each read only pays for its own block list. The explicit Flush /
        // Compaction / Cleanup calls in the Ensure*Created methods are forced
        // and not threshold-gated, so they still work and the layouts stay
        // exactly as built.
        storageConfig.SetCompactionThreshold(Max<ui32>());
        storageConfig.SetCompactionThresholdAverage(Max<ui32>());
        storageConfig.SetGarbageCompactionThreshold(Max<ui32>());
        storageConfig.SetGarbageCompactionThresholdAverage(Max<ui32>());
        storageConfig.SetCleanupThreshold(Max<ui32>());
        storageConfig.SetCleanupThresholdAverage(Max<ui32>());
        storageConfig.SetCollectGarbageThreshold(Max<ui32>());
        storageConfig.SetFlushThreshold(Max<ui32>());
        storageConfig.SetFlushBytesThreshold(Max<ui64>());

        if (ReadAheadRangeSize) {
            storageConfig.SetReadAheadCacheRangeSize(ReadAheadRangeSize);

            // Keep only the most recent widened window. The benchmarks scan
            // the same file over and over, and with the default of 32 retained
            // results the cache would hold every window of the file after the
            // first pass, so every later pass would be served entirely from
            // the cache and nothing would be described again. Retaining one
            // window makes wrapping around behave like moving forward into a
            // not yet described region, so the steady state stays at one
            // describe per window.
            storageConfig.SetReadAheadCacheMaxResultsPerNode(1);
        }

        Env.UpdateStorageConfig(std::move(storageConfig));

        Env.GetRuntime().SetDispatchedEventsLimit(Max<ui64>());

        ui32 nodeIdx = Env.AddDynamicNode();
        ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        TabletClient = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.BlockSize = BlockSize});
        TabletClient->InitSession("client", "session");

        // The files themselves are created lazily by the Ensure*Created methods
        // the first time each benchmark runs.
    }

    ~TTabletSetupT()
    {
        // HACK(svartmetal): awful hack to prevent a crash in 'verify' during
        // process cleanup:
        // https://github.com/ydb-platform/nbs/blob/6db376dbf3642aa2d869b67801fbdd929a799731/contrib/ydb/library/actors/util/local_process_key.h#L134
        _exit(0);
    }

    // Returns the distinct range ids covered by the file, ordered by the first
    // block that maps into them.
    TVector<ui32> OrderedRangeIds(ui64 nodeId) const
    {
        TVector<ui32> rangeIds;
        TSet<ui32> seen;
        for (ui32 block = 0; block < BlockCount; ++block) {
            const ui32 rangeId = GetMixedRangeIndex(nodeId, block);
            if (seen.insert(rangeId).second) {
                rangeIds.push_back(rangeId);
            }
        }
        return rangeIds;
    }

    // Forces a compaction (and cleanup) of every range covered by the file so
    // that, afterwards, each range is backed by a single blob.
    void CompactAllRanges(ui64 nodeId)
    {
        for (const ui32 rangeId: OrderedRangeIds(nodeId)) {
            TabletClient->Compaction(rangeId);
            TabletClient->Cleanup(rangeId);
        }
    }

    // Overwrites every other block, one WriteData per block so that each gets
    // its own commit id, then flushes the result into a single blob.
    void OverwriteEveryOtherBlock(ui64 handle, char fill)
    {
        for (ui32 block = 1; block < BlockCount; block += 2) {
            TabletClient->WriteData(
                handle,
                static_cast<ui64>(block) * BlockSize,
                BlockSize,
                fill);
        }
        TabletClient->Flush();
    }

    // Builds the layout shared by the mixed and multi-overwrite-mixed files: a
    // base write plus one scattered overwrite pass, compacted into a single
    // blob per range. The surviving base-commit blocks {0, 2, 4, ...} are
    // non-contiguous yet share one commit id, so they are encoded as a *mixed*
    // block group, while the overwritten blocks each have a distinct commit id
    // and become single entries. As a result iterator over TBlockList yields
    // many separate ranges for this file.
    void BuildMixedFile(ui64 handle, ui64 nodeId)
    {
        TabletClient->WriteData(handle, 0, FileSize, '0');
        OverwriteEveryOtherBlock(handle, 'x');
        CompactAllRanges(nodeId);
    }

    void EnsureMergedFileCreated()
    {
        if (MergedFileHandle) {
            return;
        }

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "merged"));

        auto handle = CreateHandle(*TabletClient, nodeId);

        TabletClient->WriteData(handle, 0, FileSize, '1');
        // Just in case.
        TabletClient->Flush();

        MergedFileHandle = handle;
    }

    void EnsureMixedFileCreated()
    {
        if (MixedFileHandle) {
            return;
        }

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "mixed"));

        auto handle = CreateHandle(*TabletClient, nodeId);

        BuildMixedFile(handle, nodeId);

        MixedFileHandle = handle;
    }

    // Creates the multi-overwrite file once: the same compacted mixed base as
    // the mixed file, plus several (up to MaxOverwrites) overlapping overwrite
    // passes layered on top. Each pass is flushed into its own blob and is
    // deliberately NOT compacted, so a range ends up backed by its base blob
    // plus up to MaxOverwrites overlapping overwrite blobs. Reading then visits
    // each overwritten block once per overlapping blob, which is what stresses
    // TBlockRangeOverlay's per-commit resolution.
    void EnsureMixedMultiOverwriteFileCreated()
    {
        if (OverwriteFileHandle) {
            return;
        }

        OverwriteNodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "mixed_multi_overwrite"));
        OverwriteFileHandle = CreateHandle(*TabletClient, OverwriteNodeId);

        BuildMixedFile(OverwriteFileHandle, OverwriteNodeId);

        const auto rangeIds = OrderedRangeIds(OverwriteNodeId);
        THashMap<ui32, ui32> overwritesByRange;
        for (ui32 i = 0; i < rangeIds.size(); ++i) {
            overwritesByRange[rangeIds[i]] =
                MaxOverwrites - (i % MaxOverwrites);
        }

        for (ui32 pass = 1; pass <= MaxOverwrites; ++pass) {
            bool wrote = false;
            for (ui32 block = 1; block < BlockCount; block += 2) {
                const ui32 rangeId =
                    GetMixedRangeIndex(OverwriteNodeId, block);
                if (pass <= overwritesByRange.at(rangeId)) {
                    TabletClient->WriteData(
                        OverwriteFileHandle,
                        static_cast<ui64>(block) * BlockSize,
                        BlockSize,
                        static_cast<char>('a' + pass));
                    wrote = true;
                }
            }

            if (wrote) {
                TabletClient->Flush();
            }
        }
    }

    // Writes the file in sub-WriteBlobThreshold chunks and does not flush, so
    // every block stays in the fresh blocks table.
    void EnsureFreshFileCreated()
    {
        if (FreshFileHandle) {
            return;
        }

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "fresh"));

        auto handle = CreateHandle(*TabletClient, nodeId);

        static_assert(FreshWriteSize < FileSize);

        for (ui64 offset = 0; offset < FileSize; offset += FreshWriteSize) {
            TabletClient->WriteData(handle, offset, FreshWriteSize, '3');
        }

        FreshFileHandle = handle;
    }

    void EnsureReadAheadFileCreated()
    {
        if (ReadAheadFileHandle) {
            return;
        }

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "read_ahead"));

        auto handle = CreateHandle(*TabletClient, nodeId);

        static_assert(FileSize < ReadAheadFileSize);

        for (ui64 offset = 0; offset < ReadAheadFileSize; offset += FileSize) {
            TabletClient->WriteData(handle, offset, FileSize, '4');
        }
        TabletClient->Flush();

        ReadAheadFileHandle = handle;
    }

    void DescribeData(
        ui64 handle,
        ui64 offset,
        ui64 length,
        ui64 expectedFileSize = FileSize)
    {
        auto response = TabletClient->DescribeData(handle, offset, length);
        Y_ABORT_UNLESS(expectedFileSize == response->Record.GetFileSize());
    }
};

using TTabletSetup = TTabletSetupT<0>;

// A 1MiB window is the smallest one that a 512KiB request can trigger:
// RegisterDescribeImpl only widens a request shorter than RangeSize.
using TReadAheadTabletSetup512KiB = TTabletSetupT<1_MB>;

// A 1MiB request needs a window wider than 1MiB to be widened at all.
using TReadAheadTabletSetup1MiB = TTabletSetupT<4_MB>;

// Ensure these singletons are destroyed before any other singletons or static
// variables to avoid a crash
constexpr ui64 SingletonPriority = Max<ui64>();

TTabletSetup* GetOrCreateTablet()
{
    return SingletonWithPriority<TTabletSetup, SingletonPriority>();
}

TReadAheadTabletSetup512KiB* GetOrCreateReadAheadTablet512KiB()
{
    return SingletonWithPriority<
        TReadAheadTabletSetup512KiB,
        SingletonPriority>();
}

TReadAheadTabletSetup1MiB* GetOrCreateReadAheadTablet1MiB()
{
    return SingletonWithPriority<
        TReadAheadTabletSetup1MiB,
        SingletonPriority>();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(TTablet_DescribeData_Merged_1MiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMergedFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MergedFileHandle, 0, 1_MB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Merged_512KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMergedFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MergedFileHandle, 0, 512_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Merged_4KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMergedFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MergedFileHandle, 0, 4_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Mixed_1MiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MixedFileHandle, 0, 1_MB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Mixed_512KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MixedFileHandle, 0, 512_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Mixed_4KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedFileCreated();

    const ui64 startOffset = 128 * 4_KB;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->MixedFileHandle, startOffset, 4_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_MixedMultiOverwrite_1MiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedMultiOverwriteFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->OverwriteFileHandle, 0, 1_MB);
    }
}

Y_CPU_BENCHMARK(
    TTablet_DescribeData_MixedMultiOverwrite_512KiB_RequestSize,
    iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedMultiOverwriteFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->OverwriteFileHandle, 0, 512_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_MixedMultiOverwrite_4KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedMultiOverwriteFileCreated();

    const ui64 startOffset = 128 * 4_KB;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->OverwriteFileHandle, startOffset, 4_KB);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fresh blocks. Nothing is blob backed, so the whole described range is
// returned inline as fresh data ranges instead of by blob reference.

Y_CPU_BENCHMARK(TTablet_DescribeData_Fresh_1MiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureFreshFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->FreshFileHandle, 0, 1_MB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Fresh_512KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureFreshFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->FreshFileHandle, 0, 512_KB);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_Fresh_4KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureFreshFileCreated();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->FreshFileHandle, 0, 4_KB);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Read ahead. The file is scanned sequentially so that the access pattern
// stays close to sequential and read ahead keeps firing: a request that
// triggers it describes the whole widened window and clips the response down,
// and the requests that fall inside that window are served from the cache.
// With a window of RangeSize and requests of RequestSize, one request out of
// every RangeSize / RequestSize describes, the rest hit the cache.

Y_CPU_BENCHMARK(TTablet_DescribeData_ReadAhead_512KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateReadAheadTablet512KiB();

    tablet->EnsureReadAheadFileCreated();

    constexpr ui64 RequestSize = 512_KB;
    constexpr ui64 PositionCount = ReadAheadFileSize / RequestSize;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(
            tablet->ReadAheadFileHandle,
            (i % PositionCount) * RequestSize,
            RequestSize,
            ReadAheadFileSize);
    }
}

Y_CPU_BENCHMARK(TTablet_DescribeData_ReadAhead_1MiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateReadAheadTablet1MiB();

    tablet->EnsureReadAheadFileCreated();

    constexpr ui64 RequestSize = 1_MB;
    constexpr ui64 PositionCount = ReadAheadFileSize / RequestSize;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(
            tablet->ReadAheadFileHandle,
            (i % PositionCount) * RequestSize,
            RequestSize,
            ReadAheadFileSize);
    }
}
