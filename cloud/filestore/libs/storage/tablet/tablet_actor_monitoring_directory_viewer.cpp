#include "tablet_actor.h"

#include <cloud/filestore/libs/service/filestore.h>
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
    TStringStream out;

    if (FAILED(args.Error.GetCode())) {
        out << JsonError(args.Error);
    } else {
        DumpDirViewerJson(out, args.Refs, args.Nodes);
    }

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(std::move(out.Str())));
}

}   // namespace NCloud::NFileStore::NStorage
