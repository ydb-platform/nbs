#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/system/file.h>

namespace {

using namespace NCloud::NBlockStore;

using NCloud::TCompressedBitmap;

////////////////////////////////////////////////////////////////////////////////

enum class ECheckReadMode : ui32
{
    MODE_NO,
    MODE_BRIEF,
    MODE_DETAILS
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    TString DiskIdsPath;
    ui64 DiskBlocks;
    ui64 ZoneBlocks;
    TString WindowSizeStr;
    TDuration WindowSize;
    TString ReportIntervalStr;
    TDuration ReportInterval;
    bool OnlyWrites;
    TString CheckReadsModeStr;
    ECheckReadMode CheckReads = ECheckReadMode::MODE_NO;
    i64 TraceBlock;
    TVector<const char*> EvlogDumperArgv;
    TString AccessedBlocksDir;
    bool AccessedBlocksStats;
    ui64 BlockSize = 4_KB;

    TOptions(int argc, const char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("evlog-dumper-params", "evlog dumper param string")
            .RequiredArgument("STR")
            .StoreResult(&EvlogDumperParamsStr);

        opts.AddLongOption("disk-id", "disk-id filter")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        opts.AddLongOption("disk-ids-path", "use multiple disk ids for filtration")
            .RequiredArgument("STR")
            .StoreResult(&DiskIdsPath);

        opts.AddLongOption("disk-blocks", "block count for the whole disk")
            .RequiredArgument("BLOCKS")
            .DefaultValue(0)
            .StoreResult(&DiskBlocks);

        opts.AddLongOption("block-size", "block size")
            .RequiredArgument("SIZE")
            .StoreResult(&BlockSize);

        opts.AddLongOption("zone-blocks", "block count per zone")
            .RequiredArgument("BLOCKS")
            .DefaultValue(32768)
            .StoreResult(&ZoneBlocks);

        opts.AddLongOption("window-size", "window size")
            .RequiredArgument("DURATION")
            .DefaultValue("60m")
            .StoreResult(&WindowSizeStr);

        opts.AddLongOption("report-interval", "report interval")
            .RequiredArgument("DURATION")
            .DefaultValue("1m")
            .StoreResult(&ReportIntervalStr);

        opts.AddLongOption("only-writes", "consider only zero and write requests")
            .NoArgument()
            .SetFlag(&OnlyWrites)
            .DefaultValue(false);

        opts.AddLongOption("accessed-stats", "dump stats about accessed blocks map")
            .NoArgument()
            .SetFlag(&AccessedBlocksStats)
            .DefaultValue(false);

        opts.AddLongOption("accessed-blocks-dir", "folder to store accessed blocks map per disk")
            .RequiredArgument("STR")
            .StoreResult(&AccessedBlocksDir);

        opts.AddLongOption("check-reads", "check reads for unwritten data")
            .RequiredArgument("MODE")
            .StoreResult(&CheckReadsModeStr);

        opts.AddLongOption("trace-block", "show all operations for block")
            .RequiredArgument("INDEX")
            .DefaultValue(-1)
            .StoreResult(&TraceBlock);

        TOptsParseResultException(&opts, argc, argv);

        WindowSize = TDuration::Parse(WindowSizeStr);
        ReportInterval = TDuration::Parse(ReportIntervalStr);

        if (CheckReadsModeStr == "brief") {
            CheckReads = ECheckReadMode::MODE_BRIEF;
        } else if (CheckReadsModeStr == "details") {
            CheckReads = ECheckReadMode::MODE_DETAILS;
        }

        if (!EvlogDumperParamsStr && !AccessedBlocksStats) {
            ythrow yexception()
                << "Either --accessed-stats or evlog-dumper-params"
                << " should be provided";
        }

        if (EvlogDumperParamsStr) {
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
    }
};

////////////////////////////////////////////////////////////////////////////////

void SerializeCompressedBitmap(const TCompressedBitmap& bitmap, IOutputStream& stream)
{
    ui64 i = 0;
    const ui64 end = bitmap.Capacity();

    while (i < end) {
        while (i < end && !bitmap.Test(i)) {
            ++i;
        }
        if (i == end) {
            break;
        }

        ui64 j = i + 1;
        while (j < end && bitmap.Test(j)) {
            ++j;
        }

        auto s = bitmap.RangeSerializer(i, j);
        TCompressedBitmap::TSerializedChunk chunk {};
        while (s.Next(&chunk)) {
            stream.Write(
                reinterpret_cast<const char*>(&chunk.ChunkIdx),
                sizeof(chunk.ChunkIdx));

            const ui32 size = static_cast<ui32>(chunk.Data.size());
            stream.Write(reinterpret_cast<const char*>(&size), sizeof(size));
            stream.Write(chunk.Data.data(), chunk.Data.size());
        }

        i = j;
    }
}

void DeserializeCompressedBitmap(
    TCompressedBitmap& bitmap,
    IZeroCopyInputFastReadTo& stream)
{
    TVector<TCompressedBitmap::TSerializedChunk> chunks;

    for (;;) {
        ui32* chunkIdx = nullptr;
        ui32* size = nullptr;

        auto len = stream.Next(&chunkIdx, sizeof(ui32));
        if (len == 0) {
            break;
        }

        Y_ABORT_UNLESS(len == sizeof(ui32));

        char* data = nullptr;

        Y_ABORT_UNLESS(stream.Next(&size, sizeof(ui32)) == sizeof(ui32));
        Y_ABORT_UNLESS(stream.Next(&data, *size) == *size);

        chunks.push_back({
            .ChunkIdx = *chunkIdx,
            .Data = TStringBuf(data, *size)
        });
    }

    for (const auto& chunk: chunks) {
        bitmap.Update(chunk);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;

    struct TDiskState
    {
        struct TZoneAccess
        {
            ui64 ZoneId = 0;
            TInstant Ts;
        };
        TList<TZoneAccess> AccessQueue;
        THashMap<ui64, ui32> Zone2Accesses;
        std::shared_ptr<TCompressedBitmap> AccessedBlocks;
        ui64 ActiveZoneCount = 0;
        TInstant LastReportTs;
        // Blocks that were read before write. Grouped per GB
        THashMap<ui64, TSet<ui64>> UnwrittenBlocks;
        ui64 BlockSize;
    };

    THashMap<TString, TDiskState> DiskId2State;

public:
    TEventProcessor(const TOptions& options)
        : Options(options)
    {
        if (Options.DiskId) {
            auto& diskState = DiskId2State[Options.DiskId];
            diskState.AccessedBlocks =
                std::make_shared<TCompressedBitmap>(Options.DiskBlocks);
            diskState.BlockSize = Options.BlockSize;
        } else {
            TIFStream is(options.DiskIdsPath);
            TString line;
            while (is.ReadLine(line)) {
                TStringBuf diskId, countAndSizeStr, blockCountStr, blockSizeStr;
                TStringBuf(line).Split('\t', diskId, countAndSizeStr);
                countAndSizeStr.Split('\t', blockCountStr, blockSizeStr);
                const auto blockCount = FromString<ui64>(blockCountStr);
                auto& diskState = DiskId2State[diskId];
                diskState.AccessedBlocks =
                    std::make_shared<TCompressedBitmap>(blockCount);
                if (!blockSizeStr) {
                    diskState.BlockSize = Options.BlockSize;
                } else {
                    diskState.BlockSize = FromString<ui64>(blockSizeStr);
                }
            }
        }

        if (options.AccessedBlocksDir) {
            LoadAccessedBlocks(options.AccessedBlocksDir);
        }
    }

    ~TEventProcessor() override
    {
        if (Options.AccessedBlocksStats) {
            for (auto& [diskId, state]: DiskId2State) {
                if (!state.AccessedBlocks) {
                    continue;
                }
                const auto diskBlocks = state.AccessedBlocks->Capacity();
                const auto accessedBlockPercentage =
                    100 * state.AccessedBlocks->Count() / diskBlocks;

                Cout << "DiskId=" << diskId
                    << "\tBlockCount=" << diskBlocks
                    << "\tAccessedBlockCount=" << state.AccessedBlocks->Count()
                    << " (" << accessedBlockPercentage << "%)"
                    << Endl;
            }
            return;
        }

        if (Options.AccessedBlocksDir) {
            SaveAccessedBlocks(Options.AccessedBlocksDir);
        }

        if (Options.CheckReads != ECheckReadMode::MODE_NO) {
            for (auto& disk: DiskId2State) {
                if (disk.second.UnwrittenBlocks.size()) {
                    auto& d = disk.second;
                    const auto blocksPerGB = 1_GB / d.BlockSize;
                    ui64 last = (d.AccessedBlocks->Capacity() - 1) / blocksPerGB;
                    for (ui64 i = 0; i <= last; ++i) {
                        if (auto it = d.UnwrittenBlocks.find(i); it != d.UnwrittenBlocks.end()) {
                            Cout << "range[" << i << " GB] has ";
                            Cout << d.UnwrittenBlocks[i].size() << " unwritten blocks read.";
                            if (Options.CheckReads == ECheckReadMode::MODE_DETAILS) {
                                Cout << " Blocks: [";
                                for (const auto& e : d.UnwrittenBlocks[i]) {
                                    Cout << e << ',';
                                }
                                Cout << ']';
                            }
                            Cout << Endl;
                        }
                    }
                }
            }
        }
    }

protected:
    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message) {
            auto* state = DiskId2State.FindPtr(message->GetDiskId());

            if (state) {
                auto order = GetItemOrder(*message);

                for (const auto& id: order) {
                    if (id.Type == EItemType::Request) {
                        const auto& r = message->GetRequests(id.Index);
                        OnRequest(r, message->GetDiskId(), *state);
                    }
                }
            }
        }
    }

private:
    void LoadAccessedBlocks(const TFsPath& dir)
    {
        for (auto& [diskId, state]: DiskId2State) {
            if (!state.AccessedBlocks) {
                continue;
            }

            const auto path = dir / (diskId + ".ab.bin");
            if (!path.Exists()) {
                continue;
            }

            const TString data = TFileInput{path}.ReadAll();
            TStringInput in{data};
            DeserializeCompressedBitmap(*state.AccessedBlocks, in);
        }
    }

    void SaveAccessedBlocks(const TFsPath& dir)
    {
        for (auto& [diskId, state]: DiskId2State) {
            if (!state.AccessedBlocks) {
                continue;
            }

            const TFsPath path = dir / (diskId + ".ab.bin");
            TFileOutput file(path);

            SerializeCompressedBitmap(*state.AccessedBlocks, file);
        }
    }

    void OnRequest(
        const NProto::TProfileLogRequestInfo& r,
        const TString& diskId,
        TDiskState& diskState)
    {
        auto start = TInstant::MicroSeconds(r.GetTimestampMcs());

        auto type =
            static_cast<EBlockStoreRequest>(r.GetRequestType());

        if (Options.OnlyWrites && !IsWriteRequest(type)) {
            return;
        }

        if (!IsReadWriteRequest(type)) {
            return;
        }

        ui64 startIndex;
        ui64 count;
        if (r.GetRanges().empty()) {
            startIndex = r.GetBlockIndex();
            count = r.GetBlockCount();
        } else {
            startIndex = r.GetRanges(0).GetBlockIndex();
            count = r.GetRanges(0).GetBlockCount();
        }

        if (Options.CheckReads != ECheckReadMode::MODE_NO) {
            if (IsWriteRequest(type)) {
                diskState.AccessedBlocks->Set(startIndex, startIndex + count);
            } else if (IsReadRequest(type)) {
                const auto blocksPerGB = 1_GB / diskState.BlockSize;
                ui64 index = startIndex;
                while (index < startIndex + count) {
                    ui64 rangeIndex = index / blocksPerGB;
                    ui64 nextRangeIndex = (rangeIndex + 1) * blocksPerGB;
                    ui64 endIndex = Min(nextRangeIndex, startIndex + count);
                    auto numWritten = diskState.AccessedBlocks->Count(index, endIndex);

                    if (numWritten < endIndex - index) {
                        for (ui64 i = index; i < endIndex; ++i) {
                            if (!diskState.AccessedBlocks->Test(i)) {
                                diskState.UnwrittenBlocks[rangeIndex].insert(i);
                            }
                        }
                    }
                    index = endIndex;
                }
            }
            return;
        }

        if (Options.TraceBlock >= 0 && IsReadWriteRequest(type)) {
            ui64 blockIndex = Options.TraceBlock;
            if ((blockIndex >= startIndex) && (blockIndex < startIndex + count)) {
                Cout << start
                    << " DiskId=" << diskId
                    << " Request=" << r.GetRequestType()
                    << " index=" << startIndex << ", "
                    << " count=" << count
                    << Endl;
            }
            return;
        }

        ui64 firstZone = startIndex / Options.ZoneBlocks;
        ui64 lastZone = (startIndex + count - 1) / Options.ZoneBlocks;
        for (ui64 z = firstZone; z <= lastZone; ++z) {
            TDiskState::TZoneAccess access{z, start};
            diskState.AccessQueue.push_back(access);
            {
                auto& accessCount = diskState.Zone2Accesses[z];
                if (!accessCount) {
                    ++diskState.ActiveZoneCount;
                }
                ++accessCount;
            }

            while (diskState.AccessQueue.front().Ts + Options.WindowSize < start) {
                auto& accessCount = diskState.Zone2Accesses[
                    diskState.AccessQueue.front().ZoneId
                ];
                --accessCount;
                if (!accessCount) {
                    diskState.Zone2Accesses.erase(
                        diskState.AccessQueue.front().ZoneId
                    );
                    --diskState.ActiveZoneCount;
                }
                diskState.AccessQueue.pop_front();
            }
        }

        diskState.AccessedBlocks->Set(startIndex, startIndex + count);

        if (diskState.LastReportTs + Options.ReportInterval < start) {
            const auto diskBlocks = diskState.AccessedBlocks->Capacity();

            const auto activePercentage =
                100 * diskState.ActiveZoneCount / (diskBlocks / Options.ZoneBlocks);
            const auto accessedBlockPercentage =
                100 * diskState.AccessedBlocks->Count() / diskBlocks;
            Cout << diskState.LastReportTs
                << "\tDiskId=" << diskId
                << "\tActiveZoneCount=" << diskState.ActiveZoneCount
                << " (" << activePercentage << "%)"
                << "\tAccessedBlockCount=" << diskState.AccessedBlocks->Count()
                << " (" << accessedBlockPercentage << "%)"
                << Endl;

            diskState.LastReportTs = start;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    try {
        TOptions options(argc, argv);
        TEventProcessor processor(options);

        if (options.EvlogDumperArgv.size()) {
            return IterateEventLog(
                NEvClass::Factory(),
                &processor,
                options.EvlogDumperArgv.size(),
                options.EvlogDumperArgv.begin()
            );
        }
    } catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
    return 0;
}
