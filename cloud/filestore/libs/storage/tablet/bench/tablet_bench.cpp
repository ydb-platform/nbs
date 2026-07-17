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

struct TTabletSetup
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

    TTabletSetup()
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

    ~TTabletSetup()
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

    void DescribeData(ui64 handle, ui64 offset, ui64 length)
    {
        auto response = TabletClient->DescribeData(handle, offset, length);
        Y_ABORT_UNLESS(FileSize == response->Record.GetFileSize());
    }
};

TTabletSetup* GetOrCreateTablet()
{
    // Ensure this singleton is destroyed before any other singletons or static
    // variables to avoid a crash
    constexpr ui64 Priority = Max<ui64>();
    return SingletonWithPriority<TTabletSetup, Priority>();
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

Y_CPU_BENCHMARK(TTablet_DescribeData_MixedMultiOverwrite_4KiB_RequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    tablet->EnsureMixedMultiOverwriteFileCreated();

    const ui64 startOffset = 128 * 4_KB;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(tablet->OverwriteFileHandle, startOffset, 4_KB);
    }
}
