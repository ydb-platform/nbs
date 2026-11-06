#include "time_range.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/logparser.h>

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

TMaybe<TRecordBounds> ReadBoundaryFrame(
    const TVector<TStringBuf>& frames,
    bool last,
    const TString& path,
    bool& reportedInvalidTimestamp)
{
    // Ignore clock-relative timestamps interpreted as Unix timestamps.
    constexpr auto MinRecordTime = TInstant::Seconds(946684800); // 2000-01-01 UTC
    bool reportedError = false;
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
                for (const auto& request: record->GetRequests()) {
                    Y_ENSURE(
                        request.HasTimestampMcs(),
                        "Missing request timestamp");
                    const auto time =
                        TInstant::MicroSeconds(request.GetTimestampMcs());
                    if (time < MinRecordTime) {
                        if (!reportedInvalidTimestamp) {
                            reportedInvalidTimestamp = true;
                            Cerr << "Invalid request timestamp in profile log "
                                 << path << ": " << time << " is before "
                                 << MinRecordTime
                                 << "; excluding it from time bounds. Request:"
                                 << Endl << request.DebugString() << Endl;
                        }
                        continue;
                    }
                    if (bounds) {
                        bounds->Start = Min(bounds->Start, time);
                        bounds->End = Max(bounds->End, time);
                    } else {
                        bounds = TRecordBounds{time, time};
                    }
                }
            }
            if (bounds) {
                return bounds;
            }
        } catch (const yexception& error) {
            if (!reportedError) {
                Cerr << "Error probing " << (last ? "last" : "first")
                     << " profile record in " << path << ": " << error.what()
                     << Endl;
                reportedError = true;
            }
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
    if (!readRecords) {
        return file;
    }
    try {
        const auto data = TBlob::FromFile(path);
        const auto frames = FindCompleteFrames(data.AsStringBuf());
        if (frames.empty()) {
            Cerr << "Cannot determine record range for " << path
                 << ": no complete eventlog frames" << Endl;
            return file;
        }
        bool reportedInvalidTimestamp = false;
        const auto first =
            ReadBoundaryFrame(frames, false, path, reportedInvalidTimestamp);
        const auto last =
            ReadBoundaryFrame(frames, true, path, reportedInvalidTimestamp);
        if (first && last) {
            file.StartTime = Min(first->Start, last->Start);
            file.EndTime = Max(first->End, last->End);
        } else {
            Cerr << "Cannot determine record range for " << path
                 << ": no usable profile records with request timestamps"
                 << Endl;
        }
    } catch (const yexception& error) {
        Cerr << "Cannot determine record range for " << path << ": "
             << error.what() << Endl;
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
        const auto& start = file.StartTime;
        const auto& fileEnd = file.EndTime;
        const bool endsAfterSince = !since || !fileEnd || *fileEnd >= *since;
        const bool startsBeforeUntil = !until || !start || *start <= *until;
        if (endsAfterSince && startsBeforeUntil) {
            Cerr << "Keeping profile log " << file.Path;
            if (start && fileEnd) {
                Cerr << ": record range " << *start << " .. " << *fileEnd
                     << " overlaps requested range";
            } else {
                Cerr << ": record range unknown; cannot exclude file";
            }
            Cerr << Endl;
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
