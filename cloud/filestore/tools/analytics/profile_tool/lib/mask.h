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

public:
    enum class EMode
    {
        NodeId,
        Hash,
        Empty,
    };

private:
    EMode Mode;

    // Some random string but stable in one session
    const TString Seed;
    ui16 MaxExtentionLength = 0;

public:
    TMaskSensitiveData(const EMode mode, const TString& seed, ui16 maxExtentionLength);
    bool Advance();
    TString Transform(const TString& str, const ui64 nodeId);
    void MaskSensitiveData(const TString& in, const TString& out);
};

}   // namespace NCloud::NFileStore::NProfileTool
