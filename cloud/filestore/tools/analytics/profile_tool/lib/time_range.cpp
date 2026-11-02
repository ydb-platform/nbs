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

TVector<TStringBuf> FindCompleteFrames(TStringBuf data)
{
    TVector<TStringBuf> frames;
    TMemoryInput input(data);
    while (input.Avail()) {
        const auto offset = data.size() - input.Avail();
        auto frame = FindNextFrame(&input, NEvClass::Factory());
        if (!frame || frame->FrameLength() > input.Avail()) {
            break;
        }
        input.Skip(frame->FrameLength());
        frames.push_back(
            data.SubStr(offset, data.size() - input.Avail() - offset));
    }
    return frames;
}

TMaybe<TRecordBounds> ReadBoundaryRecord(
    const TVector<TStringBuf>& frames,
    bool last)
{
    for (size_t i = 0; i < frames.size(); ++i) {
        TMemoryInput input(frames[last ? frames.size() - 1 - i : i]);
        try {
            auto frame = FindNextFrame(&input, NEvClass::Factory());
            if (!frame) {
                continue;
            }
            TMaybe<TRecordBounds> bounds;
            for (auto it = frame->GetIterator(); it.Avail(); it.Next()) {
                const auto event = *it;
                const auto* record =
                    dynamic_cast<const NProto::TProfileLogRecord*>(
                        event->GetProto());
                if (!record || record->GetRequests().empty()) {
                    continue;
                }
                TRecordBounds current{TInstant::Max(), TInstant::Zero()};
                for (const auto& request: record->GetRequests()) {
                    Y_ENSURE(
                        request.HasTimestampMcs(),
                        "Missing request timestamp");
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
        } catch (const yexception&) {
            // Try the next complete frame; normal processing reports read
            // errors.
        }
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
    if (!readRecords && TStringBuf(name).TryRSplit('.', base, suffix) &&
        suffix.size() == 16 &&
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
        const auto frames = FindCompleteFrames(data.AsStringBuf());
        const auto first = ReadBoundaryRecord(frames, false);
        const auto last = ReadBoundaryRecord(frames, true);
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
    if (files.size() <= 1 || (!since && !until)) {
        return files;
    }
    TVector<TProfileLogFile> result;
    for (auto& file: files) {
        const auto bounds = GetProfileLogFile(file.Path, true);
        const auto& start = bounds.StartTime;
        const auto& fileEnd = bounds.EndTime;
        const bool endsAfterSince = !since || !fileEnd || *fileEnd >= *since;
        const bool startsBeforeUntil = !until || !start || *start <= *until;
        if (endsAfterSince && startsBeforeUntil) {
            result.push_back(std::move(file));
        } else {
            Cerr << "Skipping profile log " << file.Path;
            if (!endsAfterSince) {
                Cerr << ": estimated end " << *fileEnd << " is before --since "
                     << *since;
            }
            if (!startsBeforeUntil) {
                Cerr << ": estimated start " << *start << " is after --until "
                     << *until;
            }
            Cerr << Endl;
        }
    }
    return result;
}

}   // namespace NCloud::NFileStore::NProfileTool
