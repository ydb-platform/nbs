#include "command.h"

#include <optional>

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSetNodeAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;
    std::optional<ui32> ModeAttr;
    std::optional<ui32> UidAttr;
    std::optional<ui32> GidAttr;
    std::optional<ui64> SizeAttr;
    std::optional<ui64> ATimeAttr;
    std::optional<ui64> MTimeAttr;
    std::optional<ui64> CTimeAttr;

public:
    TSetNodeAttrCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .StoreResult(&NodeId);

        Opts.AddLongOption("mode")
            .Optional()
            .StoreResult(&ModeAttr);

        Opts.AddLongOption("uid")
            .Optional()
            .StoreResult(&UidAttr);

        Opts.AddLongOption("gid")
            .Optional()
            .StoreResult(&GidAttr);

        Opts.AddLongOption("size")
            .Optional()
            .StoreResult(&SizeAttr);

        Opts.AddLongOption("atime")
            .Optional()
            .StoreResult(&ATimeAttr);

        Opts.AddLongOption("mtime")
            .Optional()
            .StoreResult(&MTimeAttr);

        Opts.AddLongOption("ctime")
            .Optional()
            .StoreResult(&CTimeAttr);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
        request->SetNodeId(NodeId);

        auto addFlag = [&request](NProto::TSetNodeAttrRequest::EFlags flag)
        {
            request->SetFlags(
                request->GetFlags() | NCloud::NFileStore::ProtoFlag(flag));
        };

        if (ModeAttr)
        {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE);
            request->MutableUpdate()->SetMode(*ModeAttr);
        }

        if (UidAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_UID);
            request->MutableUpdate()->SetUid(*UidAttr);
        }

        if (GidAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_GID);
            request->MutableUpdate()->SetGid(*GidAttr);
        }

        if (SizeAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
            request->MutableUpdate()->SetSize(*SizeAttr);
        }

        if (ATimeAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME);
            request->MutableUpdate()->SetATime(*ATimeAttr);
        }

        if (MTimeAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME);
            request->MutableUpdate()->SetMTime(*MTimeAttr);
        }

        if (CTimeAttr) {
            addFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME);
            request->MutableUpdate()->SetCTime(*CTimeAttr);
        }

        auto response = WaitFor(
            session.SetNodeAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewSetNodeAttrCommand()
{
    return std::make_shared<TSetNodeAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
