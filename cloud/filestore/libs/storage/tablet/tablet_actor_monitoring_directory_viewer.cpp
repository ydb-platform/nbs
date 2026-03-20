#include "tablet_actor.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <cloud/storage/core/libs/viewer/tablet_monitoring.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;

namespace {

////////////////////////////////////////////////////////////////////////////////

void OutputTemplate(
    const TString& templateName,
    const THashMap<TString, TString>& vars,
    IOutputStream& out)
{
    TString content = NResource::Find(
        TStringBuilder() << "html/" << templateName << ".html");
    TStringBuf contentRef(content);
    const TStringBuf b = "{{ ";
    const TStringBuf e = " }}";
    ui64 prevIdx = 0;
    while (true) {
        const ui64 idx = contentRef.find(b, prevIdx);
        if (idx == TString::npos) {
            out << contentRef.substr(prevIdx);
            break;
        }

        out << contentRef.substr(prevIdx, idx - prevIdx);
        const ui64 nextIdx = contentRef.find(e, idx + b.size());
        if (nextIdx == TString::npos) {
            out << contentRef.substr(idx);
            break;
        }

        auto varName = contentRef.substr(
            idx + b.size(),
            nextIdx - idx - b.size());
        if (const auto* varValue = vars.FindPtr(varName)) {
            out << *varValue;
        }

        prevIdx = nextIdx + e.size();
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpDirViewerJson(
    IOutputStream& out,
    const TVector<IIndexTabletDatabase::TNodeRef>& refs,
    const TVector<IIndexTabletDatabase::TNode>& nodes)
{
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    writer.BeginObject();
    writer.WriteKey("entries");
    writer.BeginList();

    size_t j = 0;
    for (const auto& ref: refs) {
        writer.BeginObject();
        writer.WriteKey("name");
        writer.WriteString(ref.Name);

        writer.WriteKey("node");
        writer.BeginObject();
        if (ref.ShardId) {
            writer.WriteKey("external");
            writer.WriteBool(true);
            writer.WriteKey("shardId");
            writer.WriteString(ref.ShardId);
            writer.WriteKey("shardNodeName");
            writer.WriteString(ref.ShardNodeName);
        } else {
            writer.WriteKey("external");
            writer.WriteBool(true);

            if (j < nodes.size()) {
                const auto& node = nodes[j++];
                writer.WriteKey("id");
                writer.WriteULongLong(node.NodeId);
                writer.WriteKey("type");
                writer.WriteULongLong(node.Attrs.GetType());
                writer.WriteKey("size");
                writer.WriteULongLong(node.Attrs.GetSize());
                writer.WriteKey("mode");
                writer.WriteULongLong(node.Attrs.GetMode());
            }
        }
        writer.EndObject();

        writer.EndObject();
    }

    writer.EndList();
    writer.EndObject();
}

void DumpDirViewerPage(IOutputStream& out, ui64 tabletId, ui32 nodeId)
{
    OutputTemplate("dir-viewer-main", {
        {"STYLE", NResource::Find("css/dir-viewer.css")},
        {"JS", NResource::Find("js/dir-viewer.js")},
        {"TABLET_ID", ToString(tabletId)},
        {"ROOT_NODE_ID", ToString(nodeId)}}, out);
}

TString JsonError(const NProto::TError& e)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    writer.BeginObject();
    writer.WriteKey("error");
    writer.WriteString(FormatError(e));
    writer.EndObject();
    return ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TGetAttrsActor final: public TActorBootstrapped<TGetAttrsActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    TVector<IIndexTabletDatabase::TNodeRef> Refs;
    TVector<IIndexTabletDatabase::TNode> Nodes;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    ui32 Responses = 0;

    TVector<TVector<ui32>> Cookie2NodeIndices;
    TVector<TString> Cookie2ShardId;

public:
    TGetAttrsActor(
        TRequestInfoPtr requestInfo,
        TVector<IIndexTabletDatabase::TNodeRef> refs,
        TVector<IIndexTabletDatabase::TNode> nodes,
        TString logTag);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void GetNodeAttrsBatch(const TActorContext& ctx);

    void HandleGetNodeAttrBatchResponse(
        const TEvIndexTablet::TEvGetNodeAttrBatchResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TGetAttrsActor::TGetAttrsActor(
        TRequestInfoPtr requestInfo,
        TVector<IIndexTabletDatabase::TNodeRef> refs,
        TVector<IIndexTabletDatabase::TNode> nodes,
        TString logTag)
    : RequestInfo(std::move(requestInfo))
    , Refs(std::move(refs))
    , Nodes(std::move(nodes))
    , LogTag(std::move(logTag))
{
}

void TGetAttrsActor::Bootstrap(const TActorContext& ctx)
{
    GetNodeAttrsBatch(ctx);
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TGetAttrsActor::GetNodeAttrsBatch(const TActorContext& ctx)
{
    struct TBatch
    {
        NProtoPrivate::TGetNodeAttrBatchRequest Record;
        TVector<ui32> NodeIndices;
    };
    THashMap<TString, TBatch> batches;

    for (ui32 i = 0; i < Refs.size(); ++i) {
        const auto& ref = Refs[i];
        if (ref.ShardId) {
            auto& batch = batches[ref.ShardId];
            if (batch.Record.GetHeaders().GetSessionId().empty()) {
                auto* headers = batch.Record.MutableHeaders();
                headers->SetBehaveAsDirectoryTablet(false);
                batch.Record.SetFileSystemId(ref.ShardId);
                batch.Record.SetNodeId(RootNodeId);
            }

            batch.Record.AddNames(ref.ShardNodeName);
            batch.NodeIndices.push_back(i);
        } else {
            ++Responses;
        }
    }

    ui64 cookie = 0;
    Cookie2NodeIndices.resize(batches.size());
    Cookie2ShardId.resize(batches.size());
    for (auto& [_, batch]: batches) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET_WORKER,
            "[%s] Executing GetNodeAttrBatch in shard for %s, %s",
            LogTag.c_str(),
            batch.Record.GetFileSystemId().c_str(),
            JoinSeq(", ", batch.Record.GetNames()).c_str());

        auto request =
            std::make_unique<TEvIndexTablet::TEvGetNodeAttrBatchRequest>();
        request->Record = std::move(batch.Record);

        Cookie2NodeIndices[cookie] = std::move(batch.NodeIndices);
        Cookie2ShardId[cookie] = request->Record.GetFileSystemId();

        // forward request through tablet proxy
        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            0, // flags
            cookie);

        ++cookie;
    }

    if (Responses == Nodes.size()) {
        ReplyAndDie(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TGetAttrsActor::HandleGetNodeAttrBatchResponse(
    const TEvIndexTablet::TEvGetNodeAttrBatchResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < Cookie2NodeIndices.size());
    const auto& nodeIndices = Cookie2NodeIndices[ev->Cookie];
    TABLET_VERIFY(ev->Cookie < Cookie2ShardId.size());
    const auto& shardId = Cookie2ShardId[ev->Cookie];

    if (HasError(msg->GetError())) {
        if (msg->GetError().GetCode() == E_FS_NOENT) {
            TStringBuilder names;
            for (auto i: nodeIndices) {
                if (names) {
                    names << ", ";
                }
                names << Refs[i].Name.Quote();
            }

            LOG_ERROR(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "Nodes not found in shard (invalid parent?): %s, %s, %s",
                shardId.c_str(),
                FormatError(msg->GetError()).Quote().c_str(),
                names.c_str());
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "Failed to GetNodeAttrBatch from shard: %s, %s",
                shardId.c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            HandleError(ctx, *msg->Record.MutableError());
            return;
        }
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "GetNodeAttrBatchResponse from shard: %s",
        msg->Record.DebugString().Quote().c_str());

    auto& responses = *msg->Record.MutableResponses();
    auto responseIter = responses.begin();
    for (const auto i: nodeIndices) {
        if (i >= Nodes.size()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << Nodes.size() << ", ShardId: "
                << shardId;
            ReportIndexOutOfBounds(message);
            LOG_ERROR(ctx, TFileStoreComponents::TABLET_WORKER, message);
            continue;
        }

        if (responseIter == responses.end()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << responses.size() << ", ShardId: " << shardId;
            ReportNotEnoughResultsInGetNodeAttrBatchResponses(message);
            LOG_ERROR(ctx, TFileStoreComponents::TABLET_WORKER, message);
            continue;
        }

        if (responseIter->GetError().GetCode() == E_FS_NOENT) {
            LOG_WARN(ctx, TFileStoreComponents::TABLET_WORKER, TStringBuilder()
                << "Node not found in shard: "
                << Refs[i].Name.Quote() << ", ShardId: "
                << shardId << ", Error: "
                << FormatError(responseIter->GetError()).Quote());
            ++responseIter;
            continue;
        }

        if (HasError(responseIter->GetError())) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "Failed to GetNodeAttr from shard: %s, %s, %s",
                Refs[i].Name.Quote().c_str(),
                shardId.c_str(),
                FormatError(responseIter->GetError()).Quote().c_str());
            ++responseIter;
            continue;
        }

        ConvertAttrsToNode(responseIter->GetNode(), &Nodes[i].Attrs);
        Nodes[i].NodeId = responseIter->GetNode().GetId();

        ++responseIter;
    }

    Responses += nodeIndices.size();
    if (Responses == Nodes.size()) {
        ReplyAndDie(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TGetAttrsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TGetAttrsActor::ReplyAndDie(const TActorContext& ctx)
{
    TStringStream out;
    DumpDirViewerJson(out, Refs, Nodes);
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(std::move(out.Str())));
    Die(ctx);
}

void TGetAttrsActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    TStringStream out;
    out << JsonError(error);
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(std::move(out.Str())));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetAttrsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvGetNodeAttrBatchResponse,
            HandleGetNodeAttrBatchResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo_DirViewer(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (params.Has("nodeId")) {
        ui64 nodeId = RootNodeId;
        if (const auto& p = params.Get("nodeId"); !TryFromString(p, nodeId)) {
            NCloud::Reply(
                ctx,
                *requestInfo,
                std::make_unique<NMon::TEvRemoteJsonInfoRes>(
                    JsonError(MakeError(E_ARGUMENT, TStringBuilder()
                        << "can't parse nodeId: " << p))));
            return;
        }

        ExecuteTx<TDirViewerListDir>(
            ctx,
            std::move(requestInfo),
            nodeId);
        return;
    }

    TStringStream out;
    DumpDirViewerPage(out, TabletID(), RootNodeId);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_DirViewerListDir(
    const TActorContext& ctx,
    TTxIndexTablet::TDirViewerListDir& args)
{
    Y_UNUSED(ctx);
    args.CommitId = GetCurrentCommitId();
    return true;
}

bool TIndexTabletActor::PrepareTx_DirViewerListDir(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TDirViewerListDir& args)
{
    Y_UNUSED(ctx);

    TMaybe<IIndexTabletDatabase::TNode> node;
    if (!ReadNode(db, args.NodeId, args.CommitId, node)) {
        return false;   // not ready
    }

    if (!node || node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    }

    TString next;
    bool ready = ReadNodeRefs(
        db,
        args.NodeId,
        args.CommitId,
        {} /* cookie */,
        args.Refs,
        Max<ui32>() /* maxBytes */,
        &next,
        false /* noAutoPrecharge */,
        NProto::LNSM_NAME_ONLY);

    if (!ready) {
        return false;
    }

    args.Nodes.reserve(args.Refs.size());
    for (const auto& ref: args.Refs) {
        if (ref.ShardId) {
            args.Nodes.emplace_back();
            continue;
        }
        TMaybe<IIndexTabletDatabase::TNode> child;
        if (!ReadNode(db, ref.ChildNodeId, args.CommitId, child)) {
            return false;
        }
        TABLET_VERIFY(child);
        args.Nodes.emplace_back(std::move(child.GetRef()));
    }

    return true;
}

void TIndexTabletActor::CompleteTx_DirViewerListDir(
    const TActorContext& ctx,
    TTxIndexTablet::TDirViewerListDir& args)
{
    auto actor = std::make_unique<TGetAttrsActor>(
        args.RequestInfo,
        std::move(args.Refs),
        std::move(args.Nodes),
        LogTag);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
