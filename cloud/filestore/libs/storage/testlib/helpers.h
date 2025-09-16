#pragma once

#include <cloud/filestore/libs/service/filestore.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct THeaders
{
    TString FileSystemId;
    TString ClientId;
    TString SessionId;
    ui64 SessionSeqNo = 0;

    template <typename T>
    void Fill(T& request) const
    {
        auto* headers = request.MutableHeaders();
        headers->SetClientId(ClientId);
        headers->SetSessionId(SessionId);
        headers->SetSessionSeqNo(SessionSeqNo);
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class ENodeType
{
    Directory,
    File,
    Link,
    SymLink,
    Sock,
    Fifo,
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateNodeArgs
{
    ENodeType NodeType;
    ui64 ParentNode = 0;
    TString Name;
    ui32 Mode = 0;

    ui64 TargetNode = 0;
    TString TargetPath;

    TString ShardId;

    TCreateNodeArgs(
            ENodeType nodeType,
            ui64 parent,
            TString name,
            TString shardId = "")
        : NodeType(nodeType)
        , ParentNode(parent)
        , Name(std::move(name))
        , ShardId(std::move(shardId))
    {}

    void Fill(NProto::TCreateNodeRequest& request) const
    {
        request.SetNodeId(ParentNode);
        request.SetName(Name);
        request.SetShardFileSystemId(ShardId);

        switch (NodeType) {
            case ENodeType::Directory: {
                auto* dir = request.MutableDirectory();
                dir->SetMode(Mode);
                break;
            }

            case ENodeType::File: {
                auto* file = request.MutableFile();
                file->SetMode(Mode);
                break;
            }

            case ENodeType::Link: {
                auto* link = request.MutableLink();
                link->SetTargetNode(TargetNode);
                break;
            }

            case ENodeType::SymLink: {
                auto* link = request.MutableSymLink();
                link->SetTargetPath(TargetPath);
                break;
            }

            case ENodeType::Sock: {
                auto* sock = request.MutableSocket();
                sock->SetMode(Mode);
                break;
            }

            case ENodeType::Fifo: {
                auto* fifo = request.MutableFifo();
                fifo->SetMode(Mode);
                break;
            }
        }
    }

    static TCreateNodeArgs Directory(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::Directory, parent, name);
        args.Mode = mode;
        return args;
    }

    static TCreateNodeArgs File(
        ui64 parent,
        const TString& name,
        ui32 mode = 0,
        const TString& shardId = "")
    {
        TCreateNodeArgs args(ENodeType::File, parent, name, shardId);
        args.Mode = mode;
        return args;
    }

    static TCreateNodeArgs Sock(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::Sock, parent, name);
        args.Mode = mode;
        return args;
    }

    static TCreateNodeArgs Link(ui64 parent, const TString& name, ui64 targetNode)
    {
        TCreateNodeArgs args(ENodeType::Link, parent, name);
        args.TargetNode = targetNode;
        return args;
    }

    static TCreateNodeArgs SymLink(ui64 parent, const TString& name, const TString& targetPath)
    {
        TCreateNodeArgs args(ENodeType::SymLink, parent, name);
        args.TargetPath = targetPath;
        return args;
    }

    static TCreateNodeArgs Fifo(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::Fifo, parent, name);
        args.Mode = mode;
        return args;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSetNodeAttrArgs
{
    ui64 Node;
    NProto::TSetNodeAttrRequest::EFlags Flags;
    NProto::TSetNodeAttrRequest::TUpdate Update;

    TSetNodeAttrArgs(ui64 node)
        : Node(node)
        , Flags(NProto::TSetNodeAttrRequest::F_NONE)
    {}

    void Fill(NProto::TSetNodeAttrRequest& request) const
    {
        request.SetNodeId(Node);
        request.SetFlags(Flags);
        request.MutableUpdate()->CopyFrom(Update);
    }

    void SetFlag(int value)
    {
        Flags = NProto::TSetNodeAttrRequest::EFlags(Flags | ProtoFlag(value));
    }

    TSetNodeAttrArgs& SetMode(ui32 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE);
        Update.SetMode(value);
        return *this;
    }

    TSetNodeAttrArgs& SetUid(ui32 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_UID);
        Update.SetUid(value);
        return *this;
    }

    TSetNodeAttrArgs& SetGid(ui32 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_GID);
        Update.SetGid(value);
        return *this;
    }

    TSetNodeAttrArgs& SetSize(ui64 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        Update.SetSize(value);
        return *this;
    }

    TSetNodeAttrArgs& SetATime(ui64 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME);
        Update.SetATime(value);
        return *this;
    }

    TSetNodeAttrArgs& SetMTime(ui64 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME);
        Update.SetMTime(value);
        return *this;
    }

    TSetNodeAttrArgs& SetCTime(ui64 value)
    {
        SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME);
        Update.SetCTime(value);
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateHandleArgs
{
    static constexpr ui32 RDNLY
        = ProtoFlag(NProto::TCreateHandleRequest::E_READ);

    static constexpr ui32 WRNLY
        = ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

    static constexpr ui32 RDWR
        = ProtoFlag(NProto::TCreateHandleRequest::E_READ)
        | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

    static constexpr ui32 CREATE
        = ProtoFlag(NProto::TCreateHandleRequest::E_CREATE)
        | ProtoFlag(NProto::TCreateHandleRequest::E_READ)
        | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

    static constexpr ui32 CREATE_EXL
        = ProtoFlag(NProto::TCreateHandleRequest::E_CREATE)
        | ProtoFlag(NProto::TCreateHandleRequest::E_EXCLUSIVE)
        | ProtoFlag(NProto::TCreateHandleRequest::E_READ)
        | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

    static constexpr ui32 TRUNC
        = ProtoFlag(NProto::TCreateHandleRequest::E_TRUNCATE);
};

////////////////////////////////////////////////////////////////////////////////

inline TString CreateBuffer(size_t len, char fill = 0)
{
    return TString(len, fill);
}

inline bool CompareBuffer(TStringBuf buffer, size_t len, char fill)
{
    if (buffer.size() != len) {
        return false;
    }

    for (size_t i = 0; i < len; ++i) {
        if (buffer[i] != fill) {
            return false;
        }
    }

    return true;
}

template <typename T>
inline bool Succeeded(const std::unique_ptr<T>& response)
{
    return response && SUCCEEDED(response->GetStatus());
}

template <typename T>
inline TString GetErrorReason(const std::unique_ptr<T>& response)
{
    if (!response) {
        return "<null response>";
    }
    if (!response->GetErrorReason()) {
        return "<no error reason>";
    }
    return response->GetErrorReason();
}

}   // namespace NCloud::NFileStore::NStorage
