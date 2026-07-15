#include "shard.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/mutex.h>

#include <util/digest/city.h>
#include <util/string/builder.h>

#include <mutex>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 NameCapacity = 20;
constexpr ui64 SlotSize = 100;
constexpr ui32 PageSize = 4_KB;
constexpr ui64 NodeTableSize = 512_MB;

struct TNodeTableSlot
{
    ui64 Id;
    ui32 Type;
    ui32 Mode;
    ui32 Uid;
    ui32 Gid;
    ui64 ATime;
    ui64 MTime;
    ui64 CTime;
    ui64 Size;
    ui32 Links;
    char Name[NameCapacity];
    ui32 RootPageNo;
};

static_assert(sizeof(TNodeTableSlot) <= SlotSize);

NProto::TNodeAttr Convert(const TNodeTableSlot& slot)
{
    NProto::TNodeAttr attr;
    attr.SetId(slot.Id);
    attr.SetType(slot.Type);
    attr.SetMode(slot.Mode);
    attr.SetUid(slot.Uid);
    attr.SetGid(slot.Gid);
    attr.SetATime(slot.ATime);
    attr.SetMTime(slot.MTime);
    attr.SetCTime(slot.CTime);
    attr.SetSize(slot.Size);
    attr.SetLinks(slot.Links);
    return attr;
}

TNodeTableSlot Convert(const TString& name, const NProto::TNodeAttr& attr)
{
    TNodeTableSlot slot{};
    slot.Id = attr.GetId();
    slot.Type = attr.GetType();
    slot.Mode = attr.GetMode();
    slot.Uid = attr.GetUid();
    slot.Gid = attr.GetGid();
    slot.ATime = attr.GetATime();
    slot.MTime = attr.GetMTime();
    slot.CTime = attr.GetCTime();
    slot.Size = attr.GetSize();
    slot.Links = attr.GetLinks();
    Y_ABORT_UNLESS(name.size() < NameCapacity);
    memcpy(slot.Name, name.data(), name.size());
    slot.Name[name.size()] = 0;
    slot.RootPageNo = 0;
    return slot;
}

////////////////////////////////////////////////////////////////////////////////
// This data structure is a prototype and is pretty inefficient:
// * no page cache - the same page is re-read multiple times
// * it uses TStrings for pages so we allocate more mem per page than needed
// So it's basically a PoC.

class TNodeTable
{
private:
    static constexpr ui64 SlotsPerPage = 40;
    static_assert(SlotsPerPage * SlotSize < PageSize);

    ui64 SlotCount = 0;
    ui64 PageCount = 0;
    IStorageGroupPtr Storage;
    mutable silk::FiberMutex ConnMutex;
    ui64 SlotPointer = 0;
    ui64 LastNodeId = 0;

public:
    void Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        IStorageGroupPtr storage)
    {
        Storage = std::move(storage);
        SlotCount = (config.GetNodesPerGroup() / SlotsPerPage) * SlotsPerPage;
        PageCount = Min(SlotCount / SlotsPerPage, NodeTableSize / PageSize);
    }

    ui64 AllocateNodeId()
    {
        // XXX
        // TODO: introduce superblock, store LastNodeId in it
        return ++LastNodeId;
    }

    NProto::TError UpdateNode(
        ui64 nodeId,
        ui32 flags,
        const NProto::TSetNodeAttrRequest::TUpdate& update,
        NProto::TNodeAttr* attr)
    {
        std::lock_guard g(ConnMutex);

        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = FindSlot(
            CityHash64(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId)),
            [=] (const TNodeTableSlot& slot) {
                return slot.Id == nodeId;
            },
            [&] () {
                return ErrorInvalidTarget(nodeId);
            },
            &slot,
            &slotNo);
        if (HasError(error)) {
            return error;
        }

        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MODE)) {
            slot.Mode = update.GetMode();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_UID)) {
            slot.Uid = update.GetUid();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_GID)) {
            slot.Gid = update.GetGid();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_ATIME)) {
            slot.ATime = update.GetATime();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_MTIME)) {
            slot.MTime = update.GetMTime();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_CTIME)) {
            slot.CTime = update.GetCTime();
        }
        if (HasFlag(flags, NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) {
            slot.Size = update.GetSize();
        }

        error = WriteNode(slot, slotNo);
        *attr = Convert(slot);
        return {};
    }

    NProto::TError PutNode(const TString& name, const NProto::TNodeAttr& attr)
    {
        std::lock_guard g(ConnMutex);

        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = FindSlot(
            CityHash64(name),
            [=] (const TNodeTableSlot& slot) {
                return strcmp(slot.Name, name.c_str()) == 0;
            },
            [&] () {
                return ErrorInvalidTarget(RootNodeId, name);
            },
            &slot,
            &slotNo);
        if (error.GetCode() != E_FS_NOENT) {
            return ErrorAlreadyExists(name);
        }

        slotNo = 0;
        error = AllocateSlot(&slotNo);
        if (HasError(error)) {
            return error;
        }

        return WriteNode(name, attr, slotNo);
    }

    NProto::TError GetNode(ui64 nodeId, NProto::TNodeAttr* attr) const
    {
        std::lock_guard g(ConnMutex);

        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = FindSlot(
            CityHash64(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId)),
            [&] (const TNodeTableSlot& slot) {
                return slot.Id == nodeId;
            },
            [&] () {
                return ErrorInvalidTarget(nodeId);
            },
            &slot,
            &slotNo);
        if (HasError(error)) {
            return error;
        }

        *attr = Convert(slot);
        return {};
    }

    NProto::TError GetNode(const TString& name, NProto::TNodeAttr* attr) const
    {
        std::lock_guard g(ConnMutex);

        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = FindSlot(
            CityHash64(name),
            [=] (const TNodeTableSlot& slot) {
                return strcmp(slot.Name, name.c_str()) == 0;
            },
            [&] () {
                return ErrorInvalidTarget(RootNodeId, name);
            },
            &slot,
            &slotNo);
        if (HasError(error)) {
            return error;
        }

        *attr = Convert(slot);
        return {};
    }

private:
    NProto::TError WritePage(ui64 slotNo, TString page)
    {
        const ui64 pageNo = slotNo / SlotsPerPage;

        NProto::TWriteLogRecordRequest request;
        auto* pg = request.AddPageGroups();
        pg->SetFirstPageNo(pageNo);
        pg->AddContent(std::move(page));

        auto response = Storage->WriteLogRecord(request);
        return response.GetError();
    }

    NProto::TError ReadPage(ui64 slotNo, TString* page) const
    {
        const ui64 pageNo = slotNo / SlotsPerPage;

        // TODO: page cache

        NProto::TReadPagesRequest request;
        auto* pg = request.AddPageGroupRefs();
        pg->SetFirstPageNo(pageNo);
        pg->SetPageCount(1);
        pg->SetPageSize(PageSize);

        auto response = Storage->ReadPages(request);
        if (HasError(response.GetError())) {
            return response.GetError();
        }

        if (response.PageGroupsSize() != 1) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected pg count: "
                    << response.PageGroupsSize());
        }

        auto& rpg = *response.MutablePageGroups(0);
        if (rpg.ContentSize() != 1) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected page count: "
                    << rpg.ContentSize());
        }

        if (rpg.GetContent(0).size() < PageSize) {
            return MakeError(
                E_BADMSG,
                TStringBuilder() << "unexpected page size: "
                    << rpg.GetContent(0).size());
        }

        *page = std::move(*rpg.MutableContent(0));
        return {};
    }

    NProto::TError LookupSlot(ui64 slotNo, TNodeTableSlot* s) const
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        memcpy(
            s,
            page.data() + relSlotNo * SlotSize,
            sizeof(TNodeTableSlot));
        return MakeError(s->Id == 0 ? S_FALSE : S_OK);
    }

    using TEq = std::function<bool(const TNodeTableSlot&)>;
    using TMakeNotFoundError = std::function<NProto::TError()>;
    NProto::TError FindSlot(
        ui64 h,
        const TEq& eq,
        const TMakeNotFoundError& nfe,
        TNodeTableSlot* s,
        ui64* slotNo) const
    {
        const ui64 firstSlotNo = h % SlotCount;
        *slotNo = firstSlotNo;
        while (true) {
            auto error = LookupSlot(*slotNo, s);
            if (HasError(error)) {
                return error;
            }

            if (error.GetCode() == S_FALSE) {
                break;
            }

            if (eq(*s)) {
                return {};
            }

            *slotNo = (*slotNo + 1) % SlotCount;
            if (*slotNo == firstSlotNo) {
                break;
            }
        }

        return nfe();
    }

    NProto::TError WriteNode(
        const TString& name,
        const NProto::TNodeAttr& attr,
        ui64 slotNo)
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        if (name.size() >= NameCapacity) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "name too long: " << name.size());
        }

        auto slot = Convert(name, attr);

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        memcpy(
            page.begin() + relSlotNo * SlotSize,
            &slot,
            sizeof(TNodeTableSlot));

        return WritePage(slotNo, std::move(page));
    }

    NProto::TError WriteNode(const TNodeTableSlot& slot, ui64 slotNo)
    {
        TString page;
        auto error = ReadPage(slotNo, &page);
        if (HasError(error)) {
            return error;
        }

        const ui32 relSlotNo = slotNo % SlotsPerPage;
        memcpy(
            page.begin() + relSlotNo * SlotSize,
            &slot,
            sizeof(TNodeTableSlot));

        return WritePage(slotNo, std::move(page));
    }

    NProto::TError AllocateSlot(ui64* slotNo)
    {
        *slotNo = SlotPointer;
        while (true) {
            TNodeTableSlot s{};
            auto error = LookupSlot(*slotNo, &s);
            if (HasError(error)) {
                return error;
            }

            if (error.GetCode() == S_FALSE) {
                break;
            }

            *slotNo = (*slotNo + 1) % SlotCount;
            if (*slotNo == SlotPointer) {
                return MakeError(E_FS_OUT_OF_SPACE, "no free node slot");
            }
        }

        SlotPointer = (*slotNo + 1) % SlotCount;
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

auto CreateAttrs(ui64 id, ui32 mode, ui64 size, ui64 uid, ui64 gid)
{
    ui64 now = MicroSeconds();

    NProto::TNodeAttr attrs;
    attrs.SetId(id);
    attrs.SetType(NProto::E_REGULAR_NODE);
    attrs.SetMode(S_IFREG | mode);
    attrs.SetATime(now);
    attrs.SetMTime(now);
    attrs.SetCTime(now);
    attrs.SetLinks(1);
    attrs.SetSize(size);
    attrs.SetUid(uid);
    attrs.SetGid(gid);

    return attrs;
}

////////////////////////////////////////////////////////////////////////////////

class TFiberShardImpl
{
private:
    const ui32 ShardNo;
    const NProtoPrivate::TPersistentFastShardConfig Config;

    IStorageGroupPtr Storage;
    TNodeTable Nodes;

public:
    TFiberShardImpl(
            ui32 shardNo,
            NProtoPrivate::TPersistentFastShardConfig config)
        : ShardNo(shardNo)
        , Config(std::move(config))
    {
        //
        // Overall it's better to pass the group into this code via dependency
        // injection but for now it's not necessary.
        //
        // Using only one storage group for now.
        //

        TVector<IStorageNodePtr> nodes;
        const auto& sg = Config.GetStorageGroups(0);
        for (const auto& d: sg.GetDevices()) {
            nodes.push_back(CreateStorageNodeClient(d.GetHost(), d.GetPort()));
        }
        Storage = CreateNaiveMirroredStorageGroup(std::move(nodes));

        Nodes.Init(Config, Storage);
    }

public:
    NProtoPrivate::TGetNodeAttrBatchResponse
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request)
    {
        NProtoPrivate::TGetNodeAttrBatchResponse response;
        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }
        for (const auto& name: request.GetNames()) {
            NProto::TNodeAttr attr;
            auto error = GetNodeAttr(request.GetNodeId(), name, &attr);
            if (HasError(error)) {
                *response.MutableError() = std::move(error);
                return response;
            }

            auto* item = response.AddResponses();
            *item->MutableNode() = std::move(attr);
        }

        return response;
    }

    NProto::TError
    GetNodeAttr(ui64 nodeId, const TString& name, NProto::TNodeAttr* attr)
    {
        if (name) {
            return Nodes.GetNode(name, attr);
        }

        return Nodes.GetNode(nodeId, attr);
    }

    NProto::TGetNodeAttrResponse
    GetNodeAttr(NProto::TGetNodeAttrRequest request)
    {
        NProto::TNodeAttr attr;
        auto error = GetNodeAttr(request.GetNodeId(), request.GetName(), &attr);
        NProto::TGetNodeAttrResponse response;
        if (HasError(error)) {
            *response.MutableError() = std::move(error);
        } else {
            *response.MutableNode() = std::move(attr);
        }

        return response;
    }

    NProto::TSetNodeAttrResponse
    SetNodeAttr(NProto::TSetNodeAttrRequest request)
    {
        NProto::TSetNodeAttrResponse response;

        auto error = Nodes.UpdateNode(
            request.GetNodeId(),
            request.GetFlags(),
            request.GetUpdate(),
            response.MutableNode());
        if (HasError(error)) {
            *response.MutableError() = std::move(error);
        }

        return response;
    }

    NProto::TAccessNodeResponse
    AccessNode(NProto::TAccessNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TCreateNodeResponse
    CreateNode(NProto::TCreateNodeRequest request)
    {
        NProto::TCreateNodeResponse response;
        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }

        if (!request.HasFile()) {
            *response.MutableError() = MakeError(
                E_NOT_IMPLEMENTED,
                "non-file create requests not supported");
            return response;
        }

        auto attr = CreateAttrs(
            ShardedId(Nodes.AllocateNodeId(), ShardNo),
            request.GetFile().GetMode(),
            0 /* size */,
            request.GetUid(),
            request.GetGid());
        auto error = Nodes.PutNode(request.GetName(), attr);
        if (HasError(error)) {
            *response.MutableError() = std::move(error);
            return response;
        }

        *response.MutableNode() = std::move(attr);
        return response;
    }

    NProto::TUnlinkNodeResponse
    UnlinkNode(NProto::TUnlinkNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TCreateHandleResponse
    CreateHandle(NProto::TCreateHandleRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TDestroyHandleResponse
    DestroyHandle(NProto::TDestroyHandleRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAllocateDataResponse
    AllocateData(NProto::TAllocateDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAcquireLockResponse
    AcquireLock(NProto::TAcquireLockRequest request)
    {
        return NotImplemented<NProto::TAcquireLockResponse>(std::move(request));
    }

    NProto::TReleaseLockResponse
    ReleaseLock(NProto::TReleaseLockRequest request)
    {
        return NotImplemented<NProto::TReleaseLockResponse>(std::move(request));
    }

    NProto::TTestLockResponse
    TestLock(NProto::TTestLockRequest request)
    {
        return NotImplemented<NProto::TTestLockResponse>(std::move(request));
    }

    NProto::TWriteDataResponse WriteData(NProto::TWriteDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TReadDataResponse ReadData(NProto::TReadDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TRemoveNodeXAttrResponse
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(request);
    }

    NProto::TGetNodeXAttrResponse
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(request);
    }

    NProto::TSetNodeXAttrResponse
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(request);
    }

    NProto::TListNodeXAttrResponse
    ListNodeXAttr(NProto::TListNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(request);
    }

private:
    template <typename TResponse, typename TRequest>
    TResponse NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define FAST_SHARD_DEFINE_METHOD(name, ns, ...)                                \
    struct TFiberShard##name##Params                                           \
    {                                                                          \
        std::shared_ptr<TFiberShardImpl> FiberShard;                           \
        std::shared_ptr<ns::T##name##Request> Request;                         \
        NThreading::TPromise<ns::T##name##Response> Promise;                   \
    };                                                                         \
                                                                               \
    int name##FiberMain(TFiberShard##name##Params* params) noexcept            \
    {                                                                          \
        auto response = params->FiberShard->name(std::move(*params->Request)); \
        params->Promise.SetValue(std::move(response));                         \
        return 0;                                                              \
    }                                                                          \
// FAST_SHARD_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DEFINE_METHOD, NProto)

#undef FAST_SHARD_DEFINE_METHOD

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

class TNaiveMirroredFileSystemShard: public IFileSystemShard
{
private:
    std::shared_ptr<TFiberShardImpl> FiberShard;

public:
    TNaiveMirroredFileSystemShard(
            ui32 shardNo,
            NProtoPrivate::TPersistentFastShardConfig config)
        : FiberShard(
            std::make_shared<TFiberShardImpl>(shardNo, std::move(config)))
    {
    }

public:
#define FAST_SHARD_DEFINE_METHOD(name, ns, ...)                                \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        auto promise = NThreading::NewPromise<ns::T##name##Response>();        \
        auto future = promise.GetFuture();                                     \
                                                                               \
        int r = silk::FiberScheduler::run(                                     \
            name##FiberMain,                                                   \
            TFiberShard##name##Params{                                         \
                .FiberShard = FiberShard,                                      \
                .Request =                                                     \
                    std::make_shared<ns::T##name##Request>(std::move(request)),\
                .Promise = promise,                                            \
            },                                                                 \
            nullptr /* future */);                                             \
        if (r) {                                                               \
            ns::T##name##Response response;                                    \
            *response.MutableError() = MakeError(E_FAIL, TStringBuilder()      \
                << "failed to spawn fiber: " << ::strerror(r));                \
            promise.SetValue(std::move(response));                             \
        }                                                                      \
                                                                               \
        return future;                                                         \
    }                                                                          \
// FAST_SHARD_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DEFINE_METHOD, NProto)

#undef FAST_SHARD_DEFINE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    return std::make_shared<TNaiveMirroredFileSystemShard>(shardNo, config);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
