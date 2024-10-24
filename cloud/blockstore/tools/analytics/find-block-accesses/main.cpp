#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/vector.h>

namespace {

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    TVector<ui64> Blocks;
    TVector<TString> BlockRangeStrs;
    TVector<TBlockRange64> BlockRanges;
    bool OutputNonIORequests = false;
    TVector<const char*> EvlogDumperArgv;

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
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        opts.AddLongOption("block-index", "block to find")
            .RequiredArgument("BLOCK")
            .AppendTo(&Blocks);

        opts.AddLongOption(
            "output-non-io-requests",
            "include non-io requests in the output")
            .NoArgument()
            .SetFlag(&OutputNonIORequests);

        opts.AddLongOption("block-range", "block ranges to find")
            .RequiredArgument("START,COUNT")
            .AppendTo(&BlockRangeStrs);

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

        for (const auto block: Blocks) {
            BlockRanges.emplace_back(TBlockRange64::MakeOneBlock(block));
        }

        for (const auto& s: BlockRangeStrs) {
            TStringBuf l, r;
            TStringBuf(s).Split(',', l, r);
            BlockRanges.push_back(TBlockRange64::WithLength(
                FromString<ui64>(l),
                FromString<ui32>(r)
            ));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;

public:
    TEventProcessor(const TOptions& options)
        : Options(options)
    {
    }

protected:
    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message && (message->GetDiskId() == Options.DiskId || !Options.DiskId)) {
            auto order = GetItemOrder(*message);

            for (const auto& id: order) {
                switch (id.Type) {
                    case EItemType::Request: {
                        ProcessRequest(*message, id.Index, out);
                        break;
                    }
                    case EItemType::BlockInfo: {
                        ProcessBlockInfo(*message, id.Index, out);
                        break;
                    }
                    case EItemType::BlockCommitId: {
                        ProcessBlockCommitId(*message, id.Index, out);
                        break;
                    }
                    case EItemType::BlobUpdate: {
                        ProcessBlobUpdate(*message, id.Index, out);
                        break;
                    }
                    default: {
                        Y_ABORT("unknown item");
                    }
                }
            }
        }
    }

private:
    void ProcessRequest(
        const NProto::TProfileLogRecord& record,
        int i,
        IOutputStream* out)
    {
        const auto& r = record.GetRequests(i);
        if (r.GetRequestType()
                < static_cast<int>(EBlockStoreRequest::MAX))
        {
            const auto t = static_cast<EBlockStoreRequest>(r.GetRequestType());
            if (t == EBlockStoreRequest::CreateCheckpoint
                    || t == EBlockStoreRequest::DeleteCheckpoint
                    || t == EBlockStoreRequest::MountVolume)
            {
                if (Options.OutputNonIORequests) {
                    DumpRequest(record, i, out);
                }

                return;
            }
        }

        for (const auto& blockRange: Options.BlockRanges) {
            bool found = false;

            if (r.GetRanges().empty()) {
                // legacy branch
                if (r.GetBlockCount()) {
                    auto reqRange = TBlockRange64::WithLength(
                        r.GetBlockIndex(),
                        r.GetBlockCount()
                    );
                    if (reqRange.Overlaps(blockRange)) {
                        found = true;
                    }
                }
            } else {
                for (const auto& range: r.GetRanges()) {
                    auto reqRange = TBlockRange64::WithLength(
                        range.GetBlockIndex(),
                        range.GetBlockCount()
                    );
                    if (reqRange.Overlaps(blockRange)) {
                        found = true;
                        break;
                    }
                }
            }

            if (found) {
                DumpRequest(record, i, out);
                break;
            }
        }
    }

    void ProcessBlockInfo(
        const NProto::TProfileLogRecord& record,
        int i,
        IOutputStream* out)
    {
        const auto& bl = record.GetBlockInfoLists(i);
        for (const auto& blockInfo: bl.GetBlockInfos()) {
            bool found = false;

            for (const auto& blockRange: Options.BlockRanges) {
                if (blockRange.Contains(blockInfo.GetBlockIndex())) {
                    found = true;
                    break;
                }
            }

            if (found) {
                DumpBlockInfoList(record, i, out);
                break;
            }
        }
    }

    void ProcessBlockCommitId(
        const NProto::TProfileLogRecord& record,
        int i,
        IOutputStream* out)
    {
        const auto& bl = record.GetBlockCommitIdLists(i);
        for (const auto& blockCommitId: bl.GetBlockCommitIds()) {
            bool found = false;

            for (const auto& blockRange: Options.BlockRanges) {
                if (blockRange.Contains(blockCommitId.GetBlockIndex())) {
                    found = true;
                    break;
                }
            }

            if (found) {
                DumpBlockCommitIdList(record, i, out);
                break;
            }
        }
    }

    void ProcessBlobUpdate(
        const NProto::TProfileLogRecord& record,
        int i,
        IOutputStream* out)
    {
        const auto& bl = record.GetBlobUpdateLists(i);
        for (const auto& blobUpdate: bl.GetBlobUpdates()) {
            bool found = false;
            auto blobUpdateRange = TBlockRange64::WithLength(
                blobUpdate.GetBlockRange().GetBlockIndex(),
                blobUpdate.GetBlockRange().GetBlockCount()
            );

            for (const auto& blockRange: Options.BlockRanges) {
                if (blockRange.Overlaps(blobUpdateRange)) {
                    found = true;
                    break;
                }
            }

            if (found) {
                DumpBlobUpdateList(record, i, out);
                break;
            }
        }
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TOptions options(argc, argv);
    TEventProcessor processor(options);

    return IterateEventLog(
        NEvClass::Factory(),
        &processor,
        options.EvlogDumperArgv.size(),
        options.EvlogDumperArgv.begin()
    );
}
