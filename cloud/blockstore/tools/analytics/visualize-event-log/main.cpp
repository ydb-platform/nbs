#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/deque.h>
#include <util/generic/ymath.h>
#include <util/string/builder.h>

namespace {

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    TVector<const char*> EvlogDumperArgv;
    TString TimeIntervalStr;
    TDuration TimeInterval;
    ui64 FirstBlockIndex;
    ui64 LastBlockIndex;
    ui32 PicWidthPixels;
    ui32 ColWidthPixels;
    ui32 BlockAxisStepPixels;
    ui32 RequestAxisStepPixels;
    bool LogScale;
    ui32 RequestsPerPixel;
    ui32 MaxImages;
    TVector<ui32> RequestTypes;

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

        opts.AddLongOption(
                "time-interval",
                "data will be split into sequential time intervals")
            .RequiredArgument("DURATION")
            .DefaultValue("15s")
            .StoreResult(&TimeIntervalStr);

        opts.AddLongOption("first-block-index", "index of the first block")
            .RequiredArgument("BLOCK_INDEX")
            .DefaultValue(0)
            .StoreResult(&FirstBlockIndex);

        opts.AddLongOption("last-block-index", "index of the last block")
            .RequiredArgument("BLOCK_INDEX")
            .Required()
            .StoreResult(&LastBlockIndex);

        opts.AddLongOption("pic-width", "width of the resulting pic")
            .RequiredArgument("PIXELS")
            .DefaultValue(1600)
            .StoreResult(&PicWidthPixels);

        opts.AddLongOption("col-width", "width of each histogram column")
            .RequiredArgument("PIXELS")
            .DefaultValue(4)
            .StoreResult(&ColWidthPixels);

        opts.AddLongOption(
                "block-axis-step-pixels",
                "distance between block index marks on the block axis")
            .RequiredArgument("PIXELS")
            .DefaultValue(100)
            .StoreResult(&BlockAxisStepPixels);

        opts.AddLongOption(
                "request-axis-step-pixels",
                "distance between request count level marks on the request axis")
            .RequiredArgument("PIXELS")
            .DefaultValue(200)
            .StoreResult(&RequestAxisStepPixels);

        opts.AddLongOption(
                "requests-per-pixel",
                "image height (affects only linear scale)")
            .RequiredArgument("REQUEST_COUNT")
            .DefaultValue(10)
            .StoreResult(&RequestsPerPixel);

        opts.AddLongOption("max-images", "stop at the specified number of images")
            .RequiredArgument("IMAGE_COUNT")
            .DefaultValue(100)
            .StoreResult(&MaxImages);

        opts.AddLongOption("log-scale", "use log scale for histogram columns")
            .NoArgument()
            .DefaultValue(false)
            .SetFlag(&LogScale);

        opts.AddLongOption("request-types", "request type filter (empty - rw filter)")
            .AppendTo(&RequestTypes);

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

        TimeInterval = TDuration::Parse(TimeIntervalStr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIntervalStat
{
    double IntervalIndex = 0;
    ui32 ReadRequests = 0;
    ui32 WriteRequests = 0;
    ui32 ZeroRequests = 0;
    ui32 CompactionRequests = 0;

    explicit operator bool() const
    {
        return ReadRequests || WriteRequests || ZeroRequests || CompactionRequests;
    }
};

bool operator<(const ui32 blockIndex, const TIntervalStat& s)
{
    return blockIndex < s.IntervalIndex;
}

struct TState
{
    TVector<TIntervalStat> Hist;
    TIntervalStat TotalStat;
    TInstant FirstTs;
};

////////////////////////////////////////////////////////////////////////////////

class TFormatter
{
private:
    class TElement
    {
    private:
        const TString Name;
        const ui32 Depth;
        TVector<std::pair<TString, TString>> Attrs;
        TString Content;
        TDeque<std::unique_ptr<TElement>> Children;

    public:
        TElement(TString name, ui32 depth = 0)
            : Name(std::move(name))
            , Depth(depth)
        {
        }

        ~TElement()
        {
            Indent();
            Cout << "<" << Name;
            for (const auto& attr: Attrs) {
                Cout << " " << attr.first << "=\"" << attr.second << "\"";
            }

            if (!Content && !Children) {
                Cout << "/>" << Endl;
            } else {
                Cout << ">" << Endl;
                if (Content) {
                    Indent();
                    Cout << " " << Content << Endl;
                }
                Children.clear();
                Indent();
                Cout << "</" << Name << ">" << Endl;
            }
        }

    private:
        void Indent()
        {
            for (ui32 i = 0; i < Depth; ++i) {
                Cout << " ";
            }
        }

    public:
        TElement& AddAttr(TString name, TString value)
        {
            Attrs.emplace_back(std::move(name), std::move(value));
            return *this;
        }

        TElement& AddChild(TString name)
        {
            Children.emplace_back(new TElement(std::move(name), Depth + 1));
            return *Children.back();
        }

        TElement& SetContent(TString content)
        {
            Content = std::move(content);
            return *this;
        }
    };

private:
    const TOptions& Options;

    std::unique_ptr<TElement> Html;
    TElement* Body = nullptr;

public:
    TFormatter(const TOptions& options)
        : Options(options)
        , Html(new TElement("html"))
    {
        Body = &Html->AddChild("body");
    }

public:
    void Finish()
    {
        Html.reset();
    }

    void Output(const TState& state) const
    {
        Body->AddChild("h3").SetContent(
            TStringBuilder() << "Time interval: "
                << state.FirstTs << " - "
                << (state.FirstTs + Options.TimeInterval)
        );

        if (state.TotalStat) {
            ui32 maxHeight = 0;
            ui32 maxRequests = 0;

            for (const auto& b: state.Hist) {
                const auto height = Height(b.ReadRequests)
                    + Height(b.WriteRequests)
                    + Height(b.ZeroRequests)
                    + Height(b.CompactionRequests);

                const auto requests = b.ReadRequests
                    + b.WriteRequests
                    + b.ZeroRequests
                    + b.CompactionRequests;

                maxHeight = Max(height, maxHeight);
                maxRequests = Max(requests, maxRequests);
            }

            const auto rightLabelsWidth = 140;
            const auto lineMargin = 5;
            const auto textHeight = 15;
            const auto bottomBarHeight = lineMargin + textHeight;
            const auto rightLineOverflow = 10;

            const auto blockCountAttr =
                ToString(Options.LastBlockIndex - Options.FirstBlockIndex);

            const auto lineWidthAttr =
                ToString(Options.PicWidthPixels + rightLineOverflow);

            auto& svg = Body->AddChild("svg")
                .AddAttr("width", ToString(Options.PicWidthPixels + rightLabelsWidth))
                .AddAttr("height", ToString(maxHeight + lineMargin + bottomBarHeight))
                ;

            // request count histogram

            ui32 currentX = 0;
            ui32 currentY = lineMargin;

            for (const auto& b: state.Hist) {
                double y = maxHeight + currentY;

                const auto rheight = Height(b.ReadRequests);
                const auto wheight = Height(b.WriteRequests);
                const auto zheight = Height(b.ZeroRequests);
                const auto cheight = Height(b.CompactionRequests);

                y -= rheight;
                svg.AddChild("rect")
                    .AddAttr("x", ToString(currentX))
                    .AddAttr("y", ToString(y))
                    .AddAttr("width", ToString(Options.ColWidthPixels))
                    .AddAttr("fill", "rgb(146, 219, 0)")
                    .AddAttr("height", ToString(ui32(rheight)))
                    ;

                y -= wheight;
                svg.AddChild("rect")
                    .AddAttr("x", ToString(currentX))
                    .AddAttr("y", ToString(y))
                    .AddAttr("width", ToString(Options.ColWidthPixels))
                    .AddAttr("fill", "rgb(0, 146, 219)")
                    .AddAttr("height", ToString(ui32(wheight)))
                    ;

                y -= zheight;
                svg.AddChild("rect")
                    .AddAttr("x", ToString(currentX))
                    .AddAttr("y", ToString(y))
                    .AddAttr("width", ToString(Options.ColWidthPixels))
                    .AddAttr("fill", "rgb(219, 183, 0)")
                    .AddAttr("height", ToString(ui32(zheight)))
                    ;

                y -= cheight;
                svg.AddChild("rect")
                    .AddAttr("x", ToString(currentX))
                    .AddAttr("y", ToString(y))
                    .AddAttr("width", ToString(Options.ColWidthPixels))
                    .AddAttr("fill", "rgb(255, 0, 0)")
                    .AddAttr("height", ToString(ui32(cheight)))
                    ;

                currentX += Options.ColWidthPixels;
            }

            // dashed lines and labels for request count levels

            {
                ui32 h = 0;

                while (h < maxHeight) {
                    svg.AddChild("line")
                        .AddAttr("x1", "0")
                        .AddAttr("y1", ToString(currentY + h))
                        .AddAttr("x2", lineWidthAttr)
                        .AddAttr("y2", ToString(currentY + h))
                        .AddAttr("stroke", "rgb(100, 100, 100)")
                        .AddAttr("stroke-dasharray", "2 1")
                        ;

                    const ui32 requestCount =
                        maxRequests * (1 - double(h) / maxHeight);

                    svg.AddChild("text")
                        .AddAttr("x", lineWidthAttr)
                        .AddAttr("y", ToString(currentY + textHeight + h))
                        .AddAttr("fill", "black")
                        .SetContent(TStringBuilder() << requestCount << " requests")
                        ;

                    h += Options.RequestAxisStepPixels;
                }
            }

            // block count line and labels

            currentY += maxHeight;
            currentY += lineMargin;

            svg.AddChild("line")
                .AddAttr("x1", "0")
                .AddAttr("y1", ToString(currentY))
                .AddAttr("x2", lineWidthAttr)
                .AddAttr("y2", ToString(currentY))
                .AddAttr("stroke", "rgb(100, 100, 100)")
                ;

            currentX = 0;

            while (true) {
                bool isLastMark = false;

                if (currentX + Options.BlockAxisStepPixels > Options.PicWidthPixels) {
                    currentX = Options.PicWidthPixels;
                    isLastMark = true;
                }

                const ui64 blockIndex = (double(currentX) / Options.PicWidthPixels)
                    * (Options.LastBlockIndex - Options.FirstBlockIndex)
                    + Options.FirstBlockIndex;

                TStringBuilder text;
                text << blockIndex;
                if (isLastMark) {
                    text << " blocks";
                }

                svg.AddChild("line")
                    .AddAttr("x1", ToString(currentX))
                    .AddAttr("y1", ToString(currentY - 3))
                    .AddAttr("x2", ToString(currentX))
                    .AddAttr("y2", ToString(currentY + 3))
                    .AddAttr("stroke", "rgb(100, 100, 100)")
                    ;

                svg.AddChild("text")
                    .AddAttr("x", ToString(currentX))
                    .AddAttr("y", ToString(currentY + textHeight))
                    .AddAttr("fill", "black")
                    .SetContent(std::move(text))
                    ;

                if (isLastMark) {
                    break;
                }

                currentX += Options.BlockAxisStepPixels;
            }
        } else {
            Body->AddChild("h3").SetContent("no requests!");
        }
    }

private:
    ui32 Height(ui32 c) const
    {
        if (c == 0) {
            return 0;
        }

        return Options.LogScale ? ceil(Log2(c)) : c / Options.RequestsPerPixel;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;
    const TFormatter& Formatter;
    TState State;
    ui32 FlushCount = 0;

public:
    TEventProcessor(const TOptions& options, const TFormatter& formatter)
        : Options(options)
        , Formatter(formatter)
    {
        const auto cols = options.PicWidthPixels / options.ColWidthPixels;
        const auto blockCount =
            options.LastBlockIndex - options.FirstBlockIndex + 1;
        const auto blocksPerCol = Max(double(blockCount) / cols, 1.);
        State.Hist.resize(cols);
        for (ui32 i = 0; i < cols; ++i) {
            State.Hist[i].IntervalIndex = i * blocksPerCol;
        }
    }

protected:
    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        if (FlushCount >= Options.MaxImages) {
            return;
        }

        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message && message->GetDiskId() == Options.DiskId) {
            struct TReq
            {
                ui32 Type;
                TInstant Ts;
                ui64 RelativeFirstBlockIndex;
                ui64 RelativeLastBlockIndex;

                bool operator<(const TReq& rhs) const
                {
                    return Ts < rhs.Ts;
                }
            };

            TVector<TReq> reqs;
            reqs.reserve(message->RequestsSize());
            for (const auto& r: message->GetRequests()) {
                const auto type = static_cast<EBlockStoreRequest>(r.GetRequestType());
                if (Options.RequestTypes) {
                    auto it = Find(
                        Options.RequestTypes.begin(),
                        Options.RequestTypes.end(),
                        r.GetRequestType()
                    );
                    if (it == Options.RequestTypes.end()) {
                        continue;
                    }
                } else if (!IsReadWriteRequest(type)) {
                    continue;
                }

                ui64 start;
                ui64 end;
                if (r.GetRanges().empty()) {
                    start = r.GetBlockIndex();
                    end = r.GetBlockIndex() + r.GetBlockCount() - 1;
                } else {
                    start = r.GetRanges(0).GetBlockIndex();
                    end = start + r.GetRanges(0).GetBlockCount() - 1;
                }

                start = Max(start, Options.FirstBlockIndex);
                end = Min(end, Options.LastBlockIndex);

                if (start <= end) {
                    reqs.push_back({
                        r.GetRequestType(),
                        TInstant::MicroSeconds(r.GetTimestampMcs()),
                        start - Options.FirstBlockIndex,
                        end - Options.FirstBlockIndex
                    });
                }
            }

            Sort(reqs.begin(), reqs.end());

            for (const auto& req: reqs) {
                if (Y_UNLIKELY(!State.FirstTs.GetValue())) {
                    State.FirstTs = req.Ts;
                } else if (State.FirstTs + Options.TimeInterval < req.Ts
                    || req.Ts < State.FirstTs) // ts overflow case
                {
                    if (!Flush()) {
                        break;
                    }

                    State.FirstTs = req.Ts;
                }

                auto bucket = UpperBound(
                    State.Hist.begin(),
                    State.Hist.end(),
                    req.RelativeFirstBlockIndex
                );
                Y_ABORT_UNLESS(bucket != State.Hist.begin());
                --bucket;

                auto endBucket = UpperBound(
                    State.Hist.begin(),
                    State.Hist.end(),
                    req.RelativeLastBlockIndex
                );

                Y_ABORT_UNLESS(endBucket != State.Hist.begin());

                while (bucket != endBucket) {
                    switch (req.Type) {
                        case ui32(EBlockStoreRequest::ReadBlocks): {
                            ++State.TotalStat.ReadRequests;
                            ++bucket->ReadRequests;
                            break;
                        }

                        case ui32(EBlockStoreRequest::WriteBlocks): {
                            ++State.TotalStat.WriteRequests;
                            ++bucket->WriteRequests;
                            break;
                        }

                        case ui32(EBlockStoreRequest::ZeroBlocks): {
                            ++State.TotalStat.ZeroRequests;
                            ++bucket->ZeroRequests;
                            break;
                        }

                        case ui32(ESysRequestType::Compaction): {
                            ++State.TotalStat.CompactionRequests;
                            ++bucket->CompactionRequests;
                            break;
                        }

                        default: {
                            Cerr << "unsupported request type: "
                                << static_cast<int>(req.Type) << Endl;
                        }
                    }

                    ++bucket;
                }
            }
        }
    }

private:
    bool Flush()
    {
        Formatter.Output(State);

        for (auto& x: State.Hist) {
            x.ReadRequests = 0;
            x.WriteRequests = 0;
            x.ZeroRequests = 0;
            x.CompactionRequests = 0;
        }

        State.TotalStat.ReadRequests = 0;
        State.TotalStat.WriteRequests = 0;
        State.TotalStat.ZeroRequests = 0;
        State.TotalStat.CompactionRequests = 0;

        return ++FlushCount < Options.MaxImages;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TOptions options(argc, argv);
    TFormatter formatter(options);
    TEventProcessor processor(options, formatter);

    auto code = IterateEventLog(
        NEvClass::Factory(),
        &processor,
        options.EvlogDumperArgv.size(),
        options.EvlogDumperArgv.begin()
    );

    formatter.Finish();

    return code;
}
