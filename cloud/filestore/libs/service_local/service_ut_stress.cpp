#include "service.h"

#include "config.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/random/random.h>

#include <random>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EStepType : ui32
{
    Truncate = 0,
    AllocateDefault,
    AllocateKeepSize,
    AllocatePunch,
    AllocateZero,
    AllocateZeroKeepSize,

    // Must be the last one.
    MAX
};

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
        request.SetFileSystemId(FileSystemId);
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
};

struct TCreateNodeArgs
{
    ui64 ParentNode = 0;
    TString Name;

    ENodeType NodeType;
    ui32 Mode = 0;

    ui64 TargetNode = 0;
    TString TargetPath;

    TCreateNodeArgs(ENodeType nodeType, ui64 parent, const TString& name)
        : ParentNode(parent)
        , Name(name)
        , NodeType(nodeType)
    {}

    void Fill(NProto::TCreateNodeRequest& request) const
    {
        request.SetNodeId(ParentNode);
        request.SetName(Name);

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

            case ENodeType::Sock: {
                auto* socket = request.MutableSocket();
                socket->SetMode(Mode);
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
        }
    }

    static TCreateNodeArgs Directory(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::Directory, parent, name);
        args.Mode = mode;
        return args;
    }

    static TCreateNodeArgs File(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::File, parent, name);
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

    static TCreateNodeArgs Sock(ui64 parent, const TString& name, ui32 mode = 0)
    {
        TCreateNodeArgs args(ENodeType::Sock, parent, name);
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
        Update.SetUid(value);
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
};

////////////////////////////////////////////////////////////////////////////////

struct TTempDirectory
    : public TFsPath
{
    TTempDirectory()
        : TFsPath(MakeName())
    {
        MkDir();
    }

    ~TTempDirectory()
    {
        try {
            ForceDelete();
        } catch (...) {
        }
    }

    static TString MakeName()
    {
        TString name = "tmp_XXXXXXXX";
        for (size_t i = 4; i < name.size(); ++i) {
            name[i] = 'a' + RandomNumber(26u);
        }

        return name;
    }
};

using TTempDirectoryPtr = std::shared_ptr<TTempDirectory>;

////////////////////////////////////////////////////////////////////////////////

struct TTestBootstrap
{
    TIntrusivePtr<TCallContext> Ctx = MakeIntrusive<TCallContext>();
    ILoggingServicePtr Logging = CreateLoggingService("console", {TLOG_RESOURCES});
    ITimerPtr Timer = CreateWallClockTimer();
    ISchedulerPtr Scheduler = CreateScheduler();
    ITaskQueuePtr TaskQueue = CreateTaskQueueStub();
    IFileIOServicePtr AIOService = CreateAIOService();

    TTempDirectoryPtr Cwd;
    IFileStoreServicePtr Store;

    THeaders Headers;

    TTestBootstrap(const TTempDirectoryPtr& cwd = std::make_shared<TTempDirectory>())
        : Cwd(cwd)
    {
        AIOService->Start();
        Store = CreateLocalFileStore(
            CreateConfig(),
            Timer,
            Scheduler,
            Logging,
            AIOService,
            TaskQueue,
            nullptr   // no profile log
        );
        Store->Start();
    }

    TTestBootstrap(const TString& id, const TString& client = "client", const TString& session = {})
        : Cwd(std::make_shared<TTempDirectory>())
    {
        AIOService->Start();
        Store = CreateLocalFileStore(
            CreateConfig(),
            Timer,
            Scheduler,
            Logging,
            AIOService,
            TaskQueue
            nullptr   // no profile log
        );
        Store->Start();

        CreateFileStore(id, "cloud", "folder", 100500, 500100);
        if (client) {
            Headers.SessionId = CreateSession(id, client, session).GetSession().GetSessionId();
        }
    }

    ~TTestBootstrap()
    {
        AIOService->Stop();
    }

    TLocalFileStoreConfigPtr CreateConfig()
    {
        NProto::TLocalServiceConfig config;
        config.SetRootPath(Cwd->GetName());

        return std::make_shared<TLocalFileStoreConfig>(config);
    }

    template<typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        Headers.Fill(*request);
        return request;
    }

    auto CreateCreateFileStoreRequest(
        const TString& id,
        const TString cloud,
        const TString folder,
        ui64 size,
        ui64 count)
    {
        auto request = CreateRequest<NProto::TCreateFileStoreRequest>();
        request->SetFileSystemId(id);
        request->SetCloudId(cloud);
        request->SetFolderId(folder);
        request->SetBlockSize(size);
        request->SetBlocksCount(count);
        return request;
    }

    auto CreateAlterFileStoreRequest(
        const TString& id,
        const TString cloud,
        const TString folder)
    {
        auto request = CreateRequest<NProto::TAlterFileStoreRequest>();
        request->SetFileSystemId(id);
        request->SetCloudId(cloud);
        request->SetFolderId(folder);
        return request;
    }

    auto CreateGetFileStoreInfoRequest(const TString& id)
    {
        auto request = CreateRequest<NProto::TGetFileStoreInfoRequest>();
        request->SetFileSystemId(id);
        return request;
    }

    auto CreateDestroyFileStoreRequest(const TString& id)
    {
        auto request = CreateRequest<NProto::TDestroyFileStoreRequest>();
        request->SetFileSystemId(id);
        return request;
    }

    void SwitchToSession(const THeaders& headers)
    {
        Headers = headers;
    }

    auto CreateCreateSessionRequest(
        const TString& id,
        const TString& clientId,
        const TString& sessionId,
        bool readOnly = false,
        ui64 seqNo = 0,
        bool restore = false)
    {
        Headers = {.FileSystemId = id, .ClientId = clientId, .SessionId = sessionId};
        auto request = CreateRequest<NProto::TCreateSessionRequest>();
        request->SetRestoreClientSession(restore);
        request->SetMountSeqNumber(seqNo);
        request->SetReadOnly(readOnly);

        return request;
    }

    auto CreateDestroySessionRequest()
    {
        return CreateRequest<NProto::TDestroySessionRequest>();
    }

    auto CreateResetSessionRequest(const TString& state)
    {
        auto request = CreateRequest<NProto::TResetSessionRequest>();
        request->SetSessionState(state);
        return request;
    }

    auto CreatePingSessionRequest()
    {
        auto request = CreateRequest<NProto::TPingSessionRequest>();
        return request;
    }

    auto CreateCreateNodeRequest(const TCreateNodeArgs& args)
    {
        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        args.Fill(*request);
        Y_ABORT_UNLESS(request->GetName());
        return request;
    }

    auto CreateUnlinkNodeRequest(ui64 parent, const TString& name, bool unlinkDirectory)
    {
        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();
        request->SetNodeId(parent);
        request->SetName(name);
        request->SetUnlinkDirectory(unlinkDirectory);
        return request;
    }

    auto CreateRenameNodeRequest(
        ui64 parent,
        const TString& name,
        ui64 newparent,
        const TString& newname,
        ui32 flags)
    {
        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        request->SetNodeId(parent);
        request->SetName(name);
        request->SetNewParentId(newparent);
        request->SetNewName(newname);
        request->SetFlags(flags);
        return request;
    }

    auto CreateAccessNodeRequest(ui64 node, int mask)
    {
        auto request = CreateRequest<NProto::TAccessNodeRequest>();
        request->SetNodeId(node);
        request->SetMask(mask);
        return request;
    }

    auto CreateReadLinkRequest(ui64 node)
    {
        auto request = CreateRequest<NProto::TReadLinkRequest>();
        request->SetNodeId(node);
        return request;
    }

    auto CreateListNodesRequest(ui64 node)
    {
        auto request = CreateRequest<NProto::TListNodesRequest>();
        request->SetNodeId(node);
        return request;
    }

    auto CreateGetNodeAttrRequest(ui64 node)
    {
        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(node);
        return request;
    }

    auto CreateGetNodeAttrRequest(ui64 node, const TString& name)
    {
        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(node);
        request->SetName(name);
        return request;
    }

    auto CreateSetNodeAttrRequest(const TSetNodeAttrArgs& args)
    {
        auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
        args.Fill(*request);
        return request;
    }

    auto CreateListNodeXAttrRequest(ui64 node)
    {
        auto request = CreateRequest<NProto::TListNodeXAttrRequest>();
        request->SetNodeId(node);
        return request;
    }

    auto CreateGetNodeXAttrRequest(ui64 node, const TString& name)
    {
        auto request = CreateRequest<NProto::TGetNodeXAttrRequest>();
        request->SetNodeId(node);
        request->SetName(name);
        return request;
    }

    auto CreateSetNodeXAttrRequest(ui64 node, const TString& name, const TString& value)
    {
        auto request = CreateRequest<NProto::TSetNodeXAttrRequest>();
        request->SetNodeId(node);
        request->SetName(name);
        request->SetValue(value);
        return request;
    }

    auto CreateRemoveNodeXAttrRequest(ui64 node, const TString& name)
    {
        auto request = CreateRequest<NProto::TRemoveNodeXAttrRequest>();
        request->SetNodeId(node);
        request->SetName(name);
        return request;
    }

    auto CreateCreateHandleRequest(ui64 parent, const TString& name, ui32 flags)
    {
        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        request->SetNodeId(parent);
        request->SetName(name);
        request->SetFlags(flags);
        return request;
    }

    auto CreateDestroyHandleRequest(ui64 handle)
    {
        auto request = CreateRequest<NProto::TDestroyHandleRequest>();
        request->SetHandle(handle);
        return request;
    }

    auto CreateWriteDataRequest(ui64 handle, ui64 offset, const TString& buffer)
    {
        auto request = CreateRequest<NProto::TWriteDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(buffer);
        return request;
    }

    auto CreateReadDataRequest(ui64 handle, ui64 offset, ui32 len)
    {
        auto request = CreateRequest<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(len);
        return request;
    }

    auto CreateAcquireLockRequest(
        ui64 handle,
        ui64 offset,
        ui32 len,
        NProto::ELockType type = NProto::E_EXCLUSIVE)
    {
        auto request = CreateRequest<NProto::TAcquireLockRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(len);
        request->SetLockType(type);
        return request;
    }

    auto CreateReleaseLockRequest(ui64 handle, ui64 offset, ui32 len)
    {
        auto request = CreateRequest<NProto::TReleaseLockRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(len);
        return request;
    }

    auto CreateTestLockRequest(ui64 handle, ui64 offset, ui32 len)
    {
        auto request = CreateRequest<NProto::TTestLockRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(len);
        return request;
    }

    auto CreatePingRequest()
    {
        return std::make_shared<NProto::TPingRequest>();
    }

    auto CreateAllocateDataRequest(ui64 handle, ui64 offset, ui64 length, ui32 flags)
    {
        auto request = CreateRequest<NProto::TAllocateDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetLength(length);
        request->SetFlags(flags);
        return request;
    }

#define FILESTORE_DECLARE_METHOD(name, ns)                                         \
    template <typename... Args>                                                    \
    NProto::T##name##Response name(Args&&... args)                                 \
    {                                                                              \
        auto request = Create##name##Request(std::forward<Args>(args)...);         \
        auto dbg = request->ShortDebugString();                                    \
        auto response = Store->name(Ctx, std::move(request)).GetValueSync();       \
                                                                                   \
        UNIT_ASSERT_C(                                                             \
            SUCCEEDED(response.GetError().GetCode()),                              \
            response.GetError().GetMessage() + "@" + dbg);                         \
        return response;                                                           \
    }                                                                              \
                                                                                   \
    template <typename... Args>                                                    \
    NProto::T##name##Response Assert##name##Failed(Args&&... args)                 \
    {                                                                              \
        auto request = Create##name##Request(std::forward<Args>(args)...);         \
        auto dbg = request->ShortDebugString();                                    \
                                                                                   \
        auto response = Store->name(Ctx, std::move(request)).GetValueSync();       \
        UNIT_ASSERT_C(                                                             \
            FAILED(response.GetError().GetCode()),                                 \
            #name " has not failed as expected " + dbg);                           \
        return response;                                                           \
    }                                                                              \
// FILESTORE_DECLARE_METHOD

    FILESTORE_SERVICE(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment
    : public NUnitTest::TBaseFixture
{
    TTestBootstrap Bootstrap = TTestBootstrap("fs");

    ILoggingServicePtr Logging = CreateLoggingService("console", { TLOG_DEBUG });
    TLog Log;

    ui64 Id = 0;
    ui64 Handle = 0;

    ui64 MaxBlocks = 0;
    ui64 BlockSize = 4_KB;
    std::mt19937_64 Engine;

    TString Data;
    char Fill = 'a';
    ui64 MaxSize = 0;

    TEnvironment() : Log(Logging->CreateLog("NFS_TEST"))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        {
            std::random_device rd;
            const auto seedValue = rd();
            Engine.seed(seedValue);
            STORAGE_DEBUG("Seed: " << seedValue);
        }
        MaxBlocks = Engine() % 64 + 1;
        STORAGE_DEBUG("Max blocks: " << MaxBlocks);

        Id = Bootstrap.CreateNode(
            TCreateNodeArgs::File(RootNodeId, "file")).GetNode().GetId();
        Handle = Bootstrap.CreateHandle(
            RootNodeId,
            "file",
            ProtoFlag(NProto::TCreateHandleRequest::E_READ)
                | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE)).GetHandle();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Bootstrap.DestroyHandle(Handle);
        Bootstrap.UnlinkNode(RootNodeId, "file", false);
    }

    char Next(char letter)
    {
        if (letter >= 'a' && letter <= 'z') {
            return letter == 'z' ? 'A' : letter + 1;
        }
        if (letter >= 'A' && letter <= 'Z') {
            return letter == 'Z' ? '0' : letter + 1;
        }
        if (letter >= '0' && letter <= '9') {
            return letter == '9' ? 'a' : letter + 1;
        }
        UNIT_ASSERT_C(false, "Unreachable code");
        return 'a';
    }

    void WriteData(ui64 offset, ui64 length)
    {
        Bootstrap.WriteData(Handle, offset, TString(Fill, length));
        Fill = Next(Fill);
    }

    TString ReadData(ui64 size, ui64 offset = 0)
    {
        return Bootstrap.ReadData(Handle, offset, size).GetBuffer();
    }

    void AllocateData(ui64 offset, ui64 length, ui32 flags)
    {
        Bootstrap.AllocateData(Handle, offset, length, flags);
    }

    NProto::TNodeAttr GetNodeAttrs()
    {
        return Bootstrap.GetNodeAttr(Id).GetNode();
    }

    void Resize(ui64 targetSize)
    {
        TSetNodeAttrArgs args(Id);
        args.SetSize(targetSize);
        Bootstrap.SetNodeAttr(std::move(args));
    }

    TString ReadFullData()
    {
        return ReadData(MaxBlocks * BlockSize);
    }

    void PerformAllocate(ui32 flags)
    {
        const ui64 currentSize = GetNodeAttrs().GetSize();

        std::uniform_int_distribution<ui64> offsetDist(0, MaxBlocks * BlockSize);
        std::uniform_int_distribution<ui64> sizeDist(1, MaxBlocks * BlockSize);

        ui64 allocOffset = 0;
        ui64 allocSize = 0;
        do {
            allocOffset = offsetDist(Engine);
            allocSize = sizeDist(Engine);
        } while (allocOffset + allocSize > MaxBlocks * BlockSize);

        AllocateData(allocOffset, allocSize, flags);

        const ui64 newSize = GetNodeAttrs().GetSize();
        MaxSize = Max<ui64>(MaxSize, newSize);

        if (HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE)) {
            UNIT_ASSERT_VALUES_EQUAL(currentSize, newSize);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                Max<ui64>(currentSize, allocOffset + allocSize),
                newSize);
        }

        const auto data = ReadFullData();
        if (const ui64 diff = Min<ui64>(allocOffset, currentSize)) {
            UNIT_ASSERT_VALUES_EQUAL(Data.substr(0, diff), data.substr(0, diff));
        }

        if (allocOffset < currentSize) {
            const ui64 rightBorder = Min<ui64>(currentSize, allocOffset + allocSize);
            if (const ui64 diff = rightBorder - allocOffset;
                HasFlag(flags, NProto::TAllocateDataRequest::F_PUNCH_HOLE) ||
                HasFlag(flags, NProto::TAllocateDataRequest::F_ZERO_RANGE))
            {
                TString expected;
                expected.resize(diff, 0);
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(allocOffset, diff));

                WriteData(allocOffset, diff);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    Data.substr(allocOffset, diff),
                    data.substr(allocOffset, diff));
            }

            if (rightBorder != currentSize) {
                const ui64 diff = currentSize - rightBorder;
                UNIT_ASSERT_VALUES_EQUAL(
                    Data.substr(rightBorder, diff),
                    data.substr(rightBorder, diff));
            } else {
                if (const ui64 diff = newSize - currentSize;
                    !HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE) && diff)
                {
                    TString expected(diff, 0);
                    UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                    WriteData(currentSize, diff);
                }
            }
        } else {
            if (const ui64 diff = newSize - currentSize;
                !HasFlag(flags, NProto::TAllocateDataRequest::F_KEEP_SIZE))
            {
                TString expected(diff, 0);
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                WriteData(currentSize, diff);
            }
        }

        Data = ReadFullData();
    }

    void PerformTruncate()
    {
        const ui64 currentSize = GetNodeAttrs().GetSize();

        std::uniform_int_distribution<ui64> sizeDist(0, MaxBlocks * BlockSize);
        const ui64 newSize = sizeDist(Engine);
        MaxSize = Max<ui64>(MaxSize, newSize);

        Resize(newSize);
        UNIT_ASSERT_VALUES_EQUAL(newSize, GetNodeAttrs().GetSize());

        const auto data = ReadFullData();
        if (newSize < currentSize) {
            UNIT_ASSERT_VALUES_EQUAL(Data.substr(0, newSize), data);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(Data, data.substr(0, currentSize));
            if (newSize != currentSize) {
                const ui64 diff = newSize - currentSize;
                TString expected(diff, 0);
                UNIT_ASSERT_VALUES_EQUAL(expected, data.substr(currentSize, diff));

                WriteData(currentSize, diff);
            }
        }

        Data = ReadFullData();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(LocalFileStore_Stress)
{
    Y_UNIT_TEST_F(ShouldStress, TEnvironment)
    {
        constexpr ui64 test_steps = 1'000;
        std::uniform_int_distribution<ui32> dist(0, EStepType::MAX - 1);

        for (size_t i = 0; i < test_steps; ++i) {
            const ui32 type = dist(Engine);
            switch (type) {
                case EStepType::Truncate: {
                    PerformTruncate();
                    break;
                }
                case EStepType::AllocateDefault: {
                    PerformAllocate(0);
                    break;
                }
                case EStepType::AllocateKeepSize: {
                    PerformAllocate(
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE));
                    break;
                }
                case EStepType::AllocatePunch: {
                    PerformAllocate(
                        ProtoFlag(NProto::TAllocateDataRequest::F_PUNCH_HOLE) |
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE));
                    break;
                }
                case EStepType::AllocateZero: {
                    PerformAllocate(
                        ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE));
                    break;
                }
                case EStepType::AllocateZeroKeepSize: {
                    PerformAllocate(
                        ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE) |
                        ProtoFlag(NProto::TAllocateDataRequest::F_KEEP_SIZE));
                    break;
                }
                default: {
                    UNIT_ASSERT_C(false, "Unreachable code");
                }
            }
        }
    }
};

}   // namespace NCloud::NFileStore
