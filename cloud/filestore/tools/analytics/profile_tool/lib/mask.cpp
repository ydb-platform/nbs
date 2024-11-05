#include "mask.h"

#include "public.h"

#include "command.h"

#include <cloud/filestore/libs/storage/tablet/profile_log_events.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/filestore/tools/analytics/libs/event-log/request_filter.h>
#include <cloud/filestore/tools/analytics/profile_tool/lib/common_filter_params.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NProfileTool {

constexpr TStringBuf OutProfileLogLabel = "out-profile-log";

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMaskCommand final: public TCommand
{
private:
    TString PathToOutProfileLog;
    TMaskSensitiveData::EMode Mode{};

public:
    TMaskCommand()
    {
        Opts.AddLongOption(
                OutProfileLogLabel.Data(),
                "Path to output profile log")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&PathToOutProfileLog);

        Opts.AddLongOption("mode", "Transform mode")
            .RequiredArgument("STR")
            .Choices({"empty", "hash", "nodeid"})
            .DefaultValue("nodeid");
    }

    bool Init(NLastGetopt::TOptsParseResultException& parseResult) override
    {
        TString modeOpt = parseResult.Get("mode");
        if (modeOpt == "nodeid") {
            Mode = TMaskSensitiveData::EMode::NodeId;
            return true;
        }

        if (modeOpt == "hash") {
            Mode = TMaskSensitiveData::EMode::Hash;
            return true;
        }

        if (modeOpt == "empty") {
            Mode = TMaskSensitiveData::EMode::Empty;
            return true;
        }

        return false;
    }

    int Execute() override
    {
        TMaskSensitiveData mask{Mode};
        mask.MaskSensitiveData(PathToProfileLog, PathToOutProfileLog);
        return 0;
    }
};

}   // namespace

TMaskSensitiveData::TMaskSensitiveData(const EMode mode)
    : Mode{mode}
{}

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
    switch (Mode) {
        case EMode::Empty: {
            return "";
        }
        case EMode::NodeId: {
            return "nodeid-" + ToString(nodeId);
        }
        case EMode::Hash: {
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

        while (EventMessageNumber > 0) {
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

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewMaskSensitiveData()
{
    return std::make_shared<TMaskCommand>();
}

}   // namespace NCloud::NFileStore::NProfileTool
