#pragma once

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/eventlog/iterator.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

class TMaskSensitiveData
{
    THolder<NEventLog::IIterator> CurrentEvent;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    const NProto::TProfileLogRecord* MessagePtr{};

    // Some random string but stable in one session
    TString Seed;

public:
    enum class EMode
    {
        Nodeid,
        Hash,
        Empty,
    };

private:
    EMode Mode;

public:
    explicit TMaskSensitiveData(const EMode& mode);
    bool Advance();
    TString Transform(const TString& str, const ui64 nodeId);
    void MaskSensitiveData(const TString& in, const TString& out);
};

}   // namespace NCloud::NFileStore::NProfileTool
