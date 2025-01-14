#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/digest/md5/md5.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString LDiff()
{
    return TString(NColorizer::StdOut().RedColor()) + "-\t";
}

TString RDiff()
{
    return TString(NColorizer::StdOut().GreenColor()) + "+\t";
}

////////////////////////////////////////////////////////////////////////////////

class TDiffCommand final
    : public TFileStoreCommand
{
private:
    TString OtherFileSystemId;
    bool ShouldDiffContent = false;

public:
    TDiffCommand()
    {
        Opts.AddLongOption("other-filesystem")
            .RequiredArgument("STR")
            .StoreResult(&OtherFileSystemId);

        Opts.AddLongOption("diff-content")
            .NoArgument()
            .SetFlag(&ShouldDiffContent);
    }

    void StatAll(
        ISession& session,
        const TString& fsId,
        TString prefix,
        ui64 parentId,
        TMap<TString, NProto::TNodeAttr>& result)
    {
        auto response = ListAll(session, fsId, parentId, false);

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            const auto subPath = prefix + name;
            result[subPath] = node;

            if (node.GetType() == NProto::E_DIRECTORY_NODE) {
                StatAll(
                    session,
                    fsId,
                    subPath + "/",
                    node.GetId(),
                    result);
            }
        }
    }

    void DumpPath(const TString& path) const
    {
        Cout << NColorizer::StdOut().DarkYellow() << "=\tPATH: "
            << path << Endl;
    }

    void DumpLine(const TString& prefix, const TString& line) const
    {
        Cout << prefix << NColorizer::StdOut().Default() << line << Endl;
    }

    TString Node2Str(const NProto::TNodeAttr& attr) const
    {
        TStringBuilder sb;
        sb << NColorizer::StdOut().Magenta()
            << "Id=" << attr.GetId()
            << " Uid=" << attr.GetUid()
            << " Gid=" << attr.GetGid()
            << " Type=" << attr.GetType()
            << " Size=" << FormatByteSize(attr.GetSize())
            << " Links=" << attr.GetLinks();
        if (attr.GetShardFileSystemId()) {
            sb << " Shard=" << attr.GetShardFileSystemId()
                << " NameInShard=" << attr.GetShardNodeName();
        }
        return sb;
    }

    void DumpNode(
        const TString& prefix,
        const TString& path,
        const NProto::TNodeAttr& attr) const
    {
        Cout << prefix << "PATH: " << NColorizer::StdOut().DarkYellow() << path
            << "\tATTR: " << Node2Str(attr) << Endl;
    }

    TString ReadNodeContent(ISession& session, const NProto::TNodeAttr& attr)
    {
        if (attr.GetSize() == 0) {
            return "";
        }

        const int flags = ProtoFlag(NProto::TCreateHandleRequest::E_READ);
        auto createRequest = CreateRequest<NProto::TCreateHandleRequest>();
        createRequest->SetNodeId(attr.GetId());
        createRequest->SetFlags(flags);

        auto createResponse = WaitFor(session.CreateHandle(
            PrepareCallContext(),
            std::move(createRequest)));

        CheckResponse(createResponse);

        auto handle = createResponse.GetHandle();

        auto readRequest = CreateRequest<NProto::TReadDataRequest>();
        readRequest->SetHandle(handle);
        readRequest->SetLength(attr.GetSize());

        auto readResponse = WaitFor(session.ReadData(
            PrepareCallContext(),
            std::move(readRequest)));

        CheckResponse(readResponse);

        return readResponse.GetBuffer();
    }

    void DiffNode(
        const TString& path,
        ISession& lsession,
        const NProto::TNodeAttr& lattr,
        ISession& rsession,
        const NProto::TNodeAttr& rattr)
    {
        auto filteredLattr = lattr;
        auto filteredRattr = rattr;
        filteredLattr.ClearATime();
        filteredLattr.ClearMTime();
        filteredLattr.ClearCTime();
        filteredLattr.ClearId();
        filteredRattr.ClearATime();
        filteredRattr.ClearMTime();
        filteredRattr.ClearCTime();
        filteredRattr.ClearId();

        const auto lattrStr = filteredLattr.Utf8DebugString().Quote();
        const auto rattrStr = filteredRattr.Utf8DebugString().Quote();
        bool dumpedPath = false;
        if (lattrStr != rattrStr) {
            DumpPath(path);
            dumpedPath = true;
            DumpLine(LDiff() + "ATTR: ", Node2Str(lattr));
            DumpLine(RDiff() + "ATTR: ", Node2Str(rattr));
        }

        if (ShouldDiffContent
                && lattr.GetType() == NProto::E_REGULAR_NODE
                && rattr.GetType() == NProto::E_REGULAR_NODE)
        {
            const auto lcontent = ReadNodeContent(lsession, lattr);
            const auto rcontent = ReadNodeContent(rsession, rattr);
            const auto lhash = MD5::Data(lcontent);
            const auto rhash = MD5::Data(rcontent);
            if (lhash != rhash) {
                if (!dumpedPath) {
                    DumpPath(path);
                }
                DumpLine(LDiff() + "DATA: ", lhash);
                DumpLine(RDiff() + "DATA: ", rhash);
            }
        }

        if (lattr.GetType() == NProto::E_LINK_NODE
                && rattr.GetType() == NProto::E_LINK_NODE)
        {
            const auto lcontent = ReadLink(lsession, lattr.GetId());
            const auto rcontent = ReadLink(rsession, rattr.GetId());
            if (lcontent != rcontent) {
                DumpLine(LDiff() + "LINK: ", lcontent);
                DumpLine(RDiff() + "LINK: ", rcontent);
            }
        }
    }

    bool Execute() override
    {
        NColorizer::StdOut().SetIsTTY(true);

        TMap<TString, NProto::TNodeAttr> lpath2Attr;
        auto lsessionGuard = CreateSession();
        auto& lsession = lsessionGuard.AccessSession();
        StatAll(lsession, FileSystemId, "/", RootNodeId, lpath2Attr);

        auto rsessionGuard = CreateCustomSession(OtherFileSystemId, ClientId);
        auto& rsession = rsessionGuard.AccessSession();
        TMap<TString, NProto::TNodeAttr> rpath2Attr;
        StatAll(rsession, OtherFileSystemId, "/", RootNodeId, rpath2Attr);

        auto lit = lpath2Attr.begin();
        auto rit = rpath2Attr.begin();

        while (lit != lpath2Attr.end() && rit != rpath2Attr.end()) {
            const auto pathCmp = lit->first.compare(rit->first);
            if (pathCmp < 0) {
                DumpNode(LDiff(), lit->first, lit->second);
                ++lit;
                continue;
            }

            if (pathCmp > 0) {
                DumpNode(RDiff(), rit->first, rit->second);
                ++rit;
                continue;
            }

            DiffNode(lit->first, lsession, lit->second, rsession, rit->second);
            ++lit;
            ++rit;
        }

        while (lit != lpath2Attr.end()) {
            DumpNode(LDiff(), lit->first, lit->second);
            ++lit;
        }

        while (rit != rpath2Attr.end()) {
            DumpNode(RDiff(), rit->first, rit->second);
            ++rit;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDiffCommand()
{
    return std::make_shared<TDiffCommand>();
}

}   // namespace NCloud::NFileStore::NClient
