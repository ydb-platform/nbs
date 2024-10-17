#include "mask.h"

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NMaskSensitiveData {

TMaskSensitiveData::TMaskSensitiveData(const TOptions& options)
    : Options{options} {};

bool TMaskSensitiveData::Advance()
{
    while (EventPtr = CurrentEvent->Next()) {
        MessagePtr = dynamic_cast<const NProto::TProfileLogRecord*>(
            EventPtr->GetProto());

        if (!MessagePtr) {
            continue;
        }

        EventMessageNumber = MessagePtr->GetRequests().size();
        return true;
    }
    return false;
}

TString TMaskSensitiveData::Transform(const TString& str, const ui64 nodeId)
{
    switch (Options.Mode) {
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

void TMaskSensitiveData::MaskSensitiveData(
    const TString& in,
    const TString& out)
{
    Seed = CreateGuidAsString();

    NEventLog::TOptions options;
    options.FileName = in;

    // Sort eventlog items by timestamp
    options.SetForceStrongOrdering(true);
    CurrentEvent = CreateIterator(options);

    TEventLog eventLog(out, 0);
    TSelfFlushLogFrame logFrame(eventLog);
    while (Advance()) {
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

}   // namespace NCloud::NFileStore::NMaskSensitiveData
