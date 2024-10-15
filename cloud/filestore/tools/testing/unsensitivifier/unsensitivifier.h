#pragma once

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/tools/testing/unsensitivifier/bootstrap.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/eventlog/iterator.h>

namespace NCloud::NFileStore::NUnsensitivifier {

////////////////////////////////////////////////////////////////////////////////

class TUnsensitivifier
{
    THolder<NEventLog::IIterator> CurrentEvent;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    const NProto::TProfileLogRecord* MessagePtr{};

    const TBootstrap& Bootstrap;

    // Some random string but stable in one session
    TString Seed;

public:
    explicit TUnsensitivifier(TBootstrap& bootstrap);
    void Advance();
    TString Transform(const TString& str, const ui64 nodeId);
    void Unsensitivifie(const TString& in, const TString& out);
};

}   // namespace NCloud::NFileStore::NUnsensitivifier
