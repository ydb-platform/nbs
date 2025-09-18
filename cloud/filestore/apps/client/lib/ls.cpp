#include "command.h"
#include "text_table.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/format.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString NameColumnName = "Name";

using TNodeId = ui64;

enum class EModeType
{
    Unknown,
    Path,
    Node,
};

enum class EOutputFormat
{
    Text,
    JSON,
};

struct TCliArgs
{
    EModeType Mode = EModeType::Unknown;

    TString Path;
    TString Cookie;
    TNodeId NodeId = 0;

    size_t RowsNumberLimit = 25;
    bool FetchAll = false;
};

////////////////////////////////////////////////////////////////////////////////

TString FileStateModeToString(ui32 mode)
{
    char permissions[] = {
        mode & S_IRUSR ? 'r' : '-',
        mode & S_IWUSR ? 'w' : '-',
        mode & S_IXUSR ? 'x' : '-',
        mode & S_IRGRP ? 'r' : '-',
        mode & S_IWGRP ? 'w' : '-',
        mode & S_IXGRP ? 'x' : '-',
        mode & S_IROTH ? 'r' : '-',
        mode & S_IWOTH ? 'w' : '-',
        mode & S_IXOTH ? 'x' : '-',
        '\0',
    };
    return permissions;
}

////////////////////////////////////////////////////////////////////////////////

struct TNodeInfo
{
    TString Name;
    NProto::TNodeAttr Node;
};

struct TPageInfo
{
    TVector<TNodeInfo> Content;
    TString Cookie;
};

////////////////////////////////////////////////////////////////////////////////

TString NodeTypeToString(NProto::ENodeType nodeType)
{
    switch (nodeType) {
        case NProto::E_INVALID_NODE:
            return "invalid";
        case NProto::E_REGULAR_NODE:
            return "f";
        case NProto::E_DIRECTORY_NODE:
            return "d";
        case NProto::E_LINK_NODE:
            return "l";
        case NProto::E_SOCK_NODE:
            return "s";
        case NProto::E_SYMLINK_NODE:
            return "L";
        case NProto::E_FIFO_NODE:
            return "p";
        default:
            ythrow yexception() << "must be unreachable";
    }
}

const TVector<TString> NodeInfoColumns = {
    "Id",
    "Type",
    "Name",
    "Mode",
    "Uid",
    "Gid",
    "ATime",
    "MTime",
    "CTime",
    "Size",
    "Links",
};

TVector<TString> ToStringVector(const TNodeInfo& nodeInfo)
{
    const auto& node = nodeInfo.Node;
    return TVector{
        ToString(node.GetId()),
        NodeTypeToString(static_cast<NProto::ENodeType>(node.GetType())),
        nodeInfo.Name,
        FileStateModeToString(node.GetMode()),
        ToString(node.GetUid()),
        ToString(node.GetGid()),
        TInstant::FromValue(node.GetATime()).ToStringUpToSeconds(),
        TInstant::FromValue(node.GetMTime()).ToStringUpToSeconds(),
        TInstant::FromValue(node.GetCTime()).ToStringUpToSeconds(),
        ToString(HumanReadableSize(node.GetSize(), SF_QUANTITY)),
        ToString(node.GetLinks()),
    };
}

NTextTable::TTextTable ToTextTable(const TVector<TNodeInfo>& nodes)
{
    auto columns = NTextTable::ToColumns(NodeInfoColumns);

    NTextTable::TTextTable table{std::move(columns)};

    for (const auto& nodeInfo: nodes) {
        table.AddRow(ToStringVector(nodeInfo));
    }
    return table;
}

NJson::TJsonValue ToJson(const TNodeInfo& node)
{
    NJson::TJsonValue json;
    NProtobufJson::Proto2Json(node.Node, json);
    json.InsertValue(NameColumnName, node.Name);
    return json;
}

NJson::TJsonValue ToJson(const TPageInfo& page)
{
    NJson::TJsonValue content(NJson::JSON_ARRAY);
    for (const auto& item: page.Content) {
        content.AppendValue(ToJson(item));
    }

    NJson::TJsonValue json(NJson::JSON_MAP);
    json.InsertValue("content", std::move(content));
    if (!page.Cookie.empty()) {
        json.InsertValue("cookie", Base64Encode(page.Cookie));
    }
    return json;
}

void Print(const TPageInfo& page, bool jsonOutput)
{
    if (jsonOutput) {
        Cout << ToJson(page) << Endl;
    } else {
        Cout << page << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TLsCommand final
    : public TFileStoreCommand
{
private:
    TCliArgs CliArgs;

public:
    TLsCommand()
    {
        const TString PathOptionName = "path";
        Opts.AddLongOption(PathOptionName)
            .RequiredArgument("PATH")
            .StoreResult(&CliArgs.Path)
            .Handler0([this] {
                CliArgs.Mode = EModeType::Path;
            });

        const TString NodeOptionName = "node";
        Opts.AddLongOption(NodeOptionName)
            .RequiredArgument("ID")
            .StoreResult(&CliArgs.NodeId)
            .Handler0([this] {
                CliArgs.Mode = EModeType::Node;
            });

        Opts.MutuallyExclusive(PathOptionName, NodeOptionName);

        Opts.AddLongOption("cookie")
            .RequiredArgument("COOKIE")
            .Handler1T<TString>([this] (const auto& cursorBase64) {
                CliArgs.Cookie = Base64Decode(cursorBase64);
            });

        const TString LimitOptionName = "limit";
        Opts.AddLongOption(LimitOptionName)
            .RequiredArgument("ROWS_NUM")
            .DefaultValue(Max<size_t>())
            .StoreResult(&CliArgs.RowsNumberLimit);

        const TString AllOptionName = "all";
        Opts.AddLongOption(AllOptionName)
            .StoreTrue(&CliArgs.FetchAll);

        Opts.MutuallyExclusive(LimitOptionName, AllOptionName);
    }

    TPageInfo FetchNodesInfo(
        ISession& session,
        const NProto::TNodeAttr& node,
        TStringBuf name = TStringBuf())
    {
        if (node.GetType() != NProto::E_DIRECTORY_NODE) {
            return TPageInfo{
                .Content = {TNodeInfo{TString(name), node}, },
                .Cookie = {},
            };
        }
        return FetchNodesInfo(session, node.GetId());
    }

    TPageInfo FetchNodesInfo(ISession& session, TNodeId nodeId)
    {
        TPageInfo page;
        page.Cookie = CliArgs.Cookie;
        do {
            auto request = CreateRequest<NProto::TListNodesRequest>();

            request->SetNodeId(nodeId);
            request->SetCookie(page.Cookie);

            auto response = WaitFor(session.ListNodes(
                PrepareCallContext(),
                std::move(request)));

            CheckResponse(response);

            const auto& names = response.GetNames();
            const auto& nodes = response.GetNodes();

            Y_ENSURE(names.size() == nodes.size(), "names/nodes sizes don't match");

            for (int i = 0; i < names.size(); ++i) {
                page.Content.push_back(TNodeInfo{names[i], nodes[i]});
            }

            page.Cookie = response.GetCookie();
        } while((CliArgs.FetchAll || page.Content.size() < CliArgs.RowsNumberLimit)
                && !page.Cookie.empty());
        return page;
    }

    TPageInfo HandlePathMode(ISession& session, TStringBuf path)
    {
        Y_ENSURE(!path.empty(), "Path must be set");

        const auto resolvedPath = ResolvePath(session, path, false).back();

        return FetchNodesInfo(session, resolvedPath.Node, resolvedPath.Name);
    }

    TPageInfo HandleNodeMode(ISession& session, TNodeId nodeId)
    {
        Y_ENSURE(nodeId != NProto::E_INVALID_NODE_ID, "Path must be set");

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(nodeId);
        request->SetName(TString{});

        auto node = WaitFor(session.GetNodeAttr(
            PrepareCallContext(),
            std::move(request))).GetNode();

        return FetchNodesInfo(session, node);
    }

    bool Execute() override
    {
        Y_ENSURE(CliArgs.Mode != EModeType::Unknown, "Mode type is not set");

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        TPageInfo page;
        switch (CliArgs.Mode) {
            case EModeType::Path:
                page = HandlePathMode(session, CliArgs.Path);
                break;
            case EModeType::Node:
                page = HandleNodeMode(session, CliArgs.NodeId);
                break;
            case EModeType::Unknown:
                break;
        }

        Print(page, JsonOutput);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewLsCommand()
{
    return std::make_shared<TLsCommand>();
}

}   // namespace NCloud::NFileStore::NClient

////////////////////////////////////////////////////////////////////////////////

template<>
inline void Out<NCloud::NFileStore::NClient::TPageInfo>(
    IOutputStream& out,
    const NCloud::NFileStore::NClient::TPageInfo& page)
{
    out << NCloud::NFileStore::NClient::ToTextTable(page.Content);
    if (!page.Cookie.empty()) {
        out << "\n\ncookie: " << Base64Encode(page.Cookie);
    }
}
