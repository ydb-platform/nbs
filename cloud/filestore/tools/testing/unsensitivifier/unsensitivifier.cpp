#include "unsensitivifier.h"

#include <library/cpp/digest/md5/md5.h>

namespace NCloud::NFileStore::NUnsensitivifier {

TUnsensitivifier::TUnsensitivifier(TBootstrap& bootstrap)
    : Bootstrap{bootstrap} {};

void TUnsensitivifier::Advance()
{
    for (EventPtr = CurrentEvent->Next(); EventPtr;
         EventPtr = CurrentEvent->Next())
    {
        MessagePtr = dynamic_cast<const NProto::TProfileLogRecord*>(
            EventPtr->GetProto());

        if (!MessagePtr) {
            return;
        }

        EventMessageNumber = MessagePtr->GetRequests().size();
        return;
    }
}

TString TUnsensitivifier::Transform(const TString& str, const ui64 nodeId)
{
    switch (Bootstrap.GetOptions()->Mode) {
        case TOptions::EMode::Empty: {
            return "";
        }
        case TOptions::EMode::Nodeid: {
            return "nodeid-" + ToString(nodeId);
        }
        case TOptions::EMode::Hash: {
            return MD5::Data(Seed + str);
        }
    }
}

void TUnsensitivifier::Unsensitivifie(const TString& in, const TString& out)
{
    Seed = ToString(random()) + ToString(random()) + ToString(random()) +
           ToString(random());

    NEventLog::TOptions options;
    options.FileName = in;

    // Sort eventlog items by timestamp
    options.SetForceStrongOrdering(true);
    CurrentEvent = CreateIterator(options);

    TEventLog eventLog(out, 0);
    TSelfFlushLogFrame logFrame(eventLog);
    for (Advance(); EventPtr; Advance()) {
        if (!MessagePtr) {
            continue;
        }

        NProto::TProfileLogRecord recordOut;
        recordOut.SetFileSystemId(MessagePtr->GetFileSystemId());

        for (; EventMessageNumber > 0;) {
            auto request = MessagePtr->GetRequests()[--EventMessageNumber];

            if (request.GetNodeInfo().HasNodeName()) {
                request.MutableNodeInfo()->SetNodeName(Transform(
                    request.GetNodeInfo().GetNodeName(),
                    request.GetNodeInfo().GetNodeId()));
            }
            if (request.GetNodeInfo().HasNewNodeName()) {
                request.MutableNodeInfo()->SetNewNodeName(Transform(
                    request.GetNodeInfo().GetNewNodeName(),
                    request.GetNodeInfo().GetNodeId()));
            }
            *recordOut.AddRequests() = std::move(request);
        }
        logFrame.LogEvent(recordOut);
    }
}

}   // namespace NCloud::NFileStore::NUnsensitivifier
