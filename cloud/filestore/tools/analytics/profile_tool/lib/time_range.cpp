#include "time_range.h"

#include "dump.h"

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
    const TString& path)
{
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
                if (bounds) {
                    bounds->Start = Min(bounds->Start, current.Start);
                    bounds->End = Max(bounds->End, current.End);
                } else {
                    bounds = current;
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
        const auto first = ReadBoundaryFrame(frames, false, path);
        const auto last = ReadBoundaryFrame(frames, true, path);
        if (first && last && first->Start <= last->End) {
            file.StartTime = first->Start;
            file.EndTime = last->End;
        } else if (!first || !last) {
            Cerr << "Cannot determine record range for " << path
                 << ": no usable profile records with request timestamps"
                 << Endl;
        } else {
            Cerr << "Cannot determine record range for " << path
                 << ": first request timestamp " << first->Start
                 << " is after last request timestamp " << last->End << Endl;
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
