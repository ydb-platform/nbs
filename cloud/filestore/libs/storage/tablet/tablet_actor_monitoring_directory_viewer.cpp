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
    const TVector<TString>& names,
    const TVector<NProto::TNodeAttr>& nodes,
    const TString& nextCookie)
{
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);

    Y_DEBUG_ABORT_UNLESS(names.size() == nodes.size());
    if (names.size() != nodes.size()) {
        writer.BeginObject();
        writer.WriteKey("error");
        writer.WriteString(TStringBuilder() << "names size " << names.size()
            << " != nodes size " << nodes.size());
        writer.EndObject();
        return;
    }

    writer.BeginObject();
    writer.WriteKey("entries");
    writer.BeginList();

    for (ui32 i = 0; i < names.size(); ++i) {
        const auto& name = names[i];
        const auto& node = nodes[i];

        writer.BeginObject();
        writer.WriteKey("name");
        writer.WriteString(name);

        {
            writer.WriteKey("node");
            writer.BeginObject();
            writer.WriteKey("shardId");
            writer.WriteString(node.GetShardFileSystemId());
            writer.WriteKey("shardNodeName");
            writer.WriteString(node.GetShardNodeName());

            writer.WriteKey("id");

            //
            // JSON parsers may convert numbers to double so not all our 64 bit
            // values would fit.
            //

            writer.WriteString(ToString(node.GetId()));
            writer.WriteKey("type");
            writer.WriteULongLong(node.GetType());
            writer.WriteKey("size");
            writer.WriteULongLong(node.GetSize());
            writer.WriteKey("mode");
            writer.WriteULongLong(node.GetMode());

            writer.EndObject();
        }

        writer.EndObject();
    }

    writer.EndList();
    if (nextCookie) {
        writer.WriteKey("nextCookie");
        writer.WriteString(nextCookie);
    }
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

/*
 * This code is similar to the TListNodesActor code in storage service but it's
 * not exactly the same and probably it will diverge more in the future so it's
 * more convenient not to generalize it and just keep a separate implementation
 * here.
 */
class TDirViewerActor final: public TActorBootstrapped<TDirViewerActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString LogTag;
    const ui64 NodeId;
    const TString DirectoryShardId;

    TVector<TString> Names;
    TVector<NProto::TNodeAttr> Nodes;
    TString NextCookie;

    // Response data
    ui32 Responses = 0;

    TVector<TVector<ui32>> Cookie2NodeIndices;
    TVector<TString> Cookie2ShardId;

public:
    TDirViewerActor(
        TRequestInfoPtr requestInfo,
        TString logTag,
        ui64 nodeId,
        TString directoryShardId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ListNodes(const TActorContext& ctx);

    void GetNodeAttrsBatch(const TActorContext& ctx);

    void HandleListNodesResponse(
        const TEvService::TEvListNodesResponse::TPtr& ev,
        const TActorContext& ctx);

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

TDirViewerActor::TDirViewerActor(
        TRequestInfoPtr requestInfo,
        TString logTag,
        ui64 nodeId,
        TString directoryShardId)
    : RequestInfo(std::move(requestInfo))
    , LogTag(std::move(logTag))
    , NodeId(nodeId)
    , DirectoryShardId(std::move(directoryShardId))
{
}

void TDirViewerActor::Bootstrap(const TActorContext& ctx)
{
    ListNodes(ctx);
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TDirViewerActor::ListNodes(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing ListNodes in directory shard %s for %lu",
        LogTag.c_str(),
        DirectoryShardId.c_str(),
        NodeId);

    auto request = std::make_unique<TEvService::TEvListNodesRequest>();
    request->Record.SetFileSystemId(DirectoryShardId);
    request->Record.SetNodeId(NodeId);
    request->Record.SetUnsafe(true);
    const ui64 sizeLimit = 128_KB;
    request->Record.SetMaxBytes(sizeLimit);
    request->Record.MutableHeaders()->SetBehaveAsDirectoryTablet(true);

    //
    // Right now DirViewer deliberately requests only the first page of each
    // directory. Think about doing the following:
    // 1. request the first and the last page
    // 2. somehow estimate and show the number of files
    //
    // OR just make 'nextCookie' clickable - to load next pages by clicking on
    // it.
    //

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

////////////////////////////////////////////////////////////////////////////////

void TDirViewerActor::GetNodeAttrsBatch(const TActorContext& ctx)
{
    struct TBatch
    {
        NProtoPrivate::TGetNodeAttrBatchRequest Record;
        TVector<ui32> NodeIndices;
    };
    THashMap<TString, TBatch> batches;

    for (ui32 i = 0; i < Nodes.size(); ++i) {
        const auto& node = Nodes[i];
        if (node.GetShardFileSystemId()) {
            auto& batch = batches[node.GetShardFileSystemId()];
            if (batch.Record.GetHeaders().GetSessionId().empty()) {
                auto* headers = batch.Record.MutableHeaders();
                headers->SetBehaveAsDirectoryTablet(false);
                batch.Record.SetFileSystemId(node.GetShardFileSystemId());
                batch.Record.SetNodeId(RootNodeId);
            }

            batch.Record.AddNames(node.GetShardNodeName());
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

void TDirViewerActor::HandleListNodesResponse(
    const TEvService::TEvListNodesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, *msg->Record.MutableError());
        return;
    }

    auto& names = *msg->Record.MutableNames();
    auto& nodes = *msg->Record.MutableNodes();
    if (names.size() != nodes.size()) {
        HandleError(ctx, MakeError(E_INVALID_STATE, TStringBuilder()
            << "names size " << names.size()
            << " != nodes size " << nodes.size()));
        return;
    }

    Names.resize(names.size());
    Nodes.resize(nodes.size());
    for (int i = 0; i < names.size(); ++i) {
        Names[i] = std::move(names[i]);
        Nodes[i] = std::move(nodes[i]);
    }
    NextCookie = msg->Record.GetCookie();

    GetNodeAttrsBatch(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDirViewerActor::HandleGetNodeAttrBatchResponse(
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
                names << Names[i].Quote();
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
                << Names[i].Quote() << ", ShardId: "
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
                Names[i].Quote().c_str(),
                shardId.c_str(),
                FormatError(responseIter->GetError()).Quote().c_str());
            ++responseIter;
            continue;
        }

        auto shardId = std::move(*Nodes[i].MutableShardFileSystemId());
        auto shardNodeName = std::move(*Nodes[i].MutableShardNodeName());
        Nodes[i] = std::move(*responseIter->MutableNode());
        Nodes[i].SetShardFileSystemId(std::move(shardId));
        Nodes[i].SetShardNodeName(std::move(shardNodeName));

        ++responseIter;
    }

    Responses += nodeIndices.size();
    if (Responses == Nodes.size()) {
        ReplyAndDie(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDirViewerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TDirViewerActor::ReplyAndDie(const TActorContext& ctx)
{
    TStringStream out;
    DumpDirViewerJson(out, Names, Nodes, NextCookie);
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(std::move(out.Str())));
    Die(ctx);
}

void TDirViewerActor::HandleError(
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

STFUNC(TDirViewerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvListNodesResponse, HandleListNodesResponse);

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
    if (!params.Has("nodeId")) {
        TStringStream out;
        DumpDirViewerPage(out, TabletID(), RootNodeId);

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
        return;
    }

    ui64 nodeId = RootNodeId;
    TString directoryShardId = GetFileSystem().GetFileSystemId();
    if (const auto& p = params.Get("nodeId"); !TryFromString(p, nodeId)) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NMon::TEvRemoteJsonInfoRes>(
                JsonError(MakeError(E_ARGUMENT, TStringBuilder()
                    << "can't parse nodeId: " << p))));
        return;
    }

    ui32 shardNo = ExtractShardNo(nodeId);
    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    if (shardNo > static_cast<ui32>(shardIds.size())) {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NMon::TEvRemoteJsonInfoRes>(
                JsonError(MakeError(E_ARGUMENT, TStringBuilder()
                    << "incorrect shardNo: " << shardNo << " > "
                    << shardIds.size()))));
        return;
    }

    if (shardNo) {
        directoryShardId = shardIds[shardNo - 1];
    }

    auto actor = std::make_unique<TDirViewerActor>(
        std::move(requestInfo),
        LogTag,
        nodeId,
        std::move(directoryShardId));

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
