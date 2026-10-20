#include "time_range.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/logparser.h>

#include <util/folder/dirut.h>
#include <util/generic/strbuf.h>
#include <util/memory/blob.h>
#include <util/stream/mem.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TRecordBounds
{
    TInstant Start;
    TInstant End;
};

TMaybe<TRecordBounds> ReadBoundaryRecord(TStringBuf data, bool last)
{
    while (!data.empty()) {
        const auto offset = last ? data.rfind(COMPRESSED_LOG_FRAME_SYNC_DATA)
                                 : data.find(COMPRESSED_LOG_FRAME_SYNC_DATA);
        if (offset == TStringBuf::npos) {
            return {};
        }
        TMemoryInput input(
            data.SubStr(offset + COMPRESSED_LOG_FRAME_SYNC_DATA.size()));
        TFrameHeader header(input);
        Y_ENSURE(
            header.FrameLength() <= input.Avail(),
            "Incomplete profile log frame");
        const auto nextOffset = data.size() - input.Avail() + header.FrameLength();
        TFrame frame(input, header, NEvClass::Factory());
        TMaybe<TRecordBounds> bounds;
        for (auto it = frame.GetIterator(); it.Avail(); it.Next()) {
            const auto event = *it;
            const auto* record = dynamic_cast<const NProto::TProfileLogRecord*>(
                event->GetProto());
            if (!record || record->GetRequests().empty()) {
                continue;
            }
            TRecordBounds current{TInstant::Max(), TInstant::Zero()};
            for (const auto& request: record->GetRequests()) {
                Y_ENSURE(request.HasTimestampMcs(), "Missing request timestamp");
                const auto time =
                    TInstant::MicroSeconds(request.GetTimestampMcs());
                current.Start = Min(current.Start, time);
                current.End = Max(current.End, time);
            }
            bounds = current;
            if (!last) {
                return bounds;
            }
        }
        if (bounds) {
            return bounds;
        }
        data = last ? data.SubStr(0, offset) : data.SubStr(nextOffset);
    }
    return {};
}

}   // namespace

TProfileLogFile GetProfileLogFile(const TString& path, bool readRecords)
{
    TProfileLogFile file{path, {}};
    const auto name = GetBaseName(path);
    // Rotated files are named <log>.YYYY-MM-DDTHH:MM (UTC, rotation time).
    TStringBuf base, suffix;
    TInstant parsedEndTime;
    if (TStringBuf(name).TryRSplit('.', base, suffix) && suffix.size() == 16 &&
        TInstant::TryParseIso8601(TString(suffix) + ":00Z", parsedEndTime))
    {
        file.EndTime = parsedEndTime;
        return file;
    }
    if (!readRecords) {
        return file;
    }
    try {
        const auto data = TBlob::FromFile(path);
        const auto first = ReadBoundaryRecord(data.AsStringBuf(), false);
        const auto last = ReadBoundaryRecord(data.AsStringBuf(), true);
        if (first && last && first->Start <= last->End) {
            file.StartTime = first->Start;
            file.EndTime = last->End;
        }
    } catch (const yexception&) {
        // Keep unreadable files eligible for normal processing/error handling.
    }
    return file;
}

TVector<TProfileLogFile> SelectProfileLogFiles(
    TVector<TProfileLogFile> files,
    const TMaybe<TInstant>& since,
    const TMaybe<TInstant>& until)
{
    constexpr auto MaxRotationDrift = TDuration::Seconds(300);
    TVector<TProfileLogFile> result;
    TMaybe<TInstant> fileStart;   // Fallback for files without a detected start.
    for (size_t i = 0; i < files.size(); ++i) {
        const bool isLast = i + 1 == files.size();
        const auto start = files[i].StartTime ? files[i].StartTime : fileStart;
        const auto fileEnd = isLast && !files[i].StartTime
                                 ? Nothing()
                                 : files[i].EndTime;
        const bool endsAfterSince =
            !since || !fileEnd || *fileEnd + MaxRotationDrift >= *since;
        const bool startsBeforeUntil =
            !until || !start || *start <= *until + MaxRotationDrift;
        fileStart = files[i].EndTime;
        if (endsAfterSince && startsBeforeUntil) {
            result.push_back(std::move(files[i]));
        } else {
            Cerr << "Skipping profile log " << files[i].Path;
            if (!endsAfterSince) {
                Cerr << ": estimated end " << *fileEnd << " + "
                     << MaxRotationDrift.Seconds() << "s is before --since "
                     << *since;
            }
            if (!startsBeforeUntil) {
                Cerr << ": estimated start " << *start << " is after --until "
                     << *until << " + " << MaxRotationDrift.Seconds() << "s";
            }
            Cerr << Endl;
        }
    }
    return result;
}

}   // namespace NCloud::NFileStore::NProfileTool
