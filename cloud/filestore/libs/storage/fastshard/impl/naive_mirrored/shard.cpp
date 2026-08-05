#include "shard.h"

#include "page_store.h"
#include "persistent_bitmap.h"
#include "persistent_hash_table.h"

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
#include <util/random/random.h>
#include <util/string/builder.h>

#include <sys/stat.h>

#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////
// inode table layout

constexpr ui64 NodeSlotSize = 96;   // bigger than the current slot struct - in
                                    // order not to drop all data if we decide
                                    // to add something to the slot struct
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
};

static_assert(sizeof(TNodeTableSlot) <= NodeSlotSize);
static_assert(NodeSlotSize % alignof(TNodeTableSlot) == 0);

////////////////////////////////////////////////////////////////////////////////
// name table layout

constexpr ui64 NameSlotSize = 48;
constexpr ui32 NameCapacity = 36;

struct TNameTableSlot
{
    char Name[NameCapacity];
    ui64 NodeId;
};

static_assert(sizeof(TNameTableSlot) <= NameSlotSize);

////////////////////////////////////////////////////////////////////////////////
// handle table layout

constexpr ui64 HandleSlotSize = 16;

struct THandleSlot
{
    ui64 Handle;
    ui64 NodeId;
};

static_assert(sizeof(THandleSlot) <= HandleSlotSize);

////////////////////////////////////////////////////////////////////////////////
// page index layout

constexpr ui64 PageClusterPageCount = 8;
constexpr ui64 PageClusterSize = PageClusterPageCount * PageSize;
constexpr ui64 NodePageClusterSlotSize = 24;
constexpr ui64 MaxSpacePerStorageGroup = 100_GB;
constexpr ui64 MaxNodePageClusterTableSlotCount =
    MaxSpacePerStorageGroup / PageClusterSize;
constexpr ui64 MaxNodePageClusterTableSize =
    MaxNodePageClusterTableSlotCount * NodePageClusterSlotSize;
constexpr ui64 InvalidStoragePageClusterId = Max<ui64>();

static_assert(MaxNodePageClusterTableSize == 75_MB);

struct TNodePageClusterKey
{
    ui64 NodeId;
    ui64 PageClusterId;

    bool operator==(const TNodePageClusterKey& rhs) const
    {
        return NodeId == rhs.NodeId && PageClusterId == rhs.PageClusterId;
    }
};

struct TNodePageClusterSlot
{
    TNodePageClusterKey Key;
    ui64 StoragePageClusterId;
};

static_assert(sizeof(TNodePageClusterSlot) <= NodePageClusterSlotSize);

////////////////////////////////////////////////////////////////////////////////

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

TNodeTableSlot Convert(const NProto::TNodeAttr& attr)
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
    return slot;
}

////////////////////////////////////////////////////////////////////////////////

ui64 RoundUp(ui64 n, ui64 by)
{
    return ((n - 1) / by + 1) * by;
}

ui64 RoundDown(ui64 n, ui64 by)
{
    return (n / by) * by;
}

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext
{
    NProto::TDeviceRequestHeaders Headers;
    TVector<TPageGroup> PageGroups;
    ui64 Lsn = 0;
    bool PagesCollected = false;
};

TVector<ui64> CollectPages(TWriteContext& writeContext)
{
    TVector<ui64> pages;
    for (const auto& pg: writeContext.PageGroups) {
        for (ui64 i = 0; i < pg.Content.size(); ++i) {
            pages.push_back(pg.FirstPageNo + i);
        }
    }

    writeContext.PagesCollected = true;
    return pages;
}

class TWriteContextGuard
{
private:
    TWriteContext& Context;
    IPageStore& Store;

public:
    TWriteContextGuard(TWriteContext& context, IPageStore& store)
        : Context(context)
        , Store(store)
    {
        Context.Lsn = Store.AllocateLsn();
    }

    ~TWriteContextGuard()
    {
        if (!Context.PagesCollected) {
            auto pages = CollectPages(Context);
            Store.RollbackPages(pages);
        }

        // TODO(#5895) - notify storage that this Lsn was skipped
    }
};

////////////////////////////////////////////////////////////////////////////////
// This data structure is a PoC, it's not really efficient, can be easily
// optimized.

class TNodeTable
{
private:
    static constexpr ui64 SlotsPerPage = 42;
    static_assert(SlotsPerPage * NodeSlotSize <= PageSize);

    using THt = TPersistentHashTable<ui64, TNodeTableSlot>;
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 firstPageNo,
        IPageStorePtr pageStore)
    {
        const ui64 pageCount =
            Min(RoundUp(config.GetNodesPerGroup(), SlotsPerPage),
                (NodeTableSize / PageSize) * SlotsPerPage) /
            SlotsPerPage;
        const TNodeTableSlot tombstone{.Id = Max<ui64>()};
        Slots = std::make_unique<THt>(
            firstPageNo,
            pageCount,
            PageSize,
            NodeSlotSize,
            tombstone,
            std::move(pageStore),
            [](const TNodeTableSlot& s) -> ui64 { return s.Id; },
            [](const ui64& nodeId) -> ui64 {
                return CityHash64(
                    reinterpret_cast<const char*>(&nodeId),
                    sizeof(nodeId));
            });

        return pageCount;
    }

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError AllocateNodeId(ui64* nodeId)
    {
        while (true) {
            *nodeId = ShardedId(RandomNumber<ui64>(), 0 /* shardNo */);
            NProto::TNodeAttr attr;
            auto error = GetNode(*nodeId, &attr);
            if (!HasError(error)) {
                continue;
            }

            if (error.GetCode() == E_FS_NOENT) {
                return {};
            }

            return error;
        }
    }

    NProto::TError
    ResizeNode(ui64 nodeId, ui64 newSize, TWriteContext& writeContext)
    {
        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = Slots->Get(writeContext.Lsn, nodeId, &slot, &slotNo);
        if (HasError(error)) {
            return error;
        }

        slot.Size = Max(slot.Size, newSize);

        return Slots
            ->Update(writeContext.Lsn, slot, slotNo, writeContext.PageGroups);
    }

    NProto::TError UpdateNode(
        ui64 nodeId,
        ui32 flags,
        const NProto::TSetNodeAttrRequest::TUpdate& update,
        NProto::TNodeAttr* attr,
        TWriteContext& writeContext)
    {
        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = Slots->Get(writeContext.Lsn, nodeId, &slot, &slotNo);
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
            if (slot.Size > update.GetSize()) {
                //
                // TODO(#5894): deallocate pages and delete them from page index
                //
            }

            slot.Size = update.GetSize();
        }

        error = Slots->Update(
            writeContext.Lsn,
            slot,
            slotNo,
            writeContext.PageGroups);
        if (HasError(error)) {
            return error;
        }

        *attr = Convert(slot);
        return {};
    }

    NProto::TError PutNode(
        const NProto::TNodeAttr& attr,
        TWriteContext& writeContext)
    {
        auto slot = Convert(attr);
        return Slots->Put(writeContext.Lsn, slot, writeContext.PageGroups);
    }

    NProto::TError DeleteNode(ui64 nodeId, TWriteContext& writeContext)
    {
        TNodeTableSlot slot{};
        return Slots
            ->Delete(writeContext.Lsn, nodeId, &slot, writeContext.PageGroups);
    }

    NProto::TError GetNode(ui64 nodeId, NProto::TNodeAttr* attr) const
    {
        TNodeTableSlot slot{};
        ui64 slotNo = 0;
        auto error = Slots->Get(0 /* lsn */, nodeId, &slot, &slotNo);
        if (HasError(error)) {
            return error;
        }

        *attr = Convert(slot);
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNameTable
{
private:
    static constexpr ui64 SlotsPerPage = PageSize / NameSlotSize;
    static_assert(SlotsPerPage * NameSlotSize <= PageSize);

    using THt = TPersistentHashTable<TStringBuf, TNameTableSlot>;
    TNameTableSlot Tombstone{};
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 firstPageNo,
        IPageStorePtr pageStore)
    {
        const ui64 pageCount =
            Min(RoundUp(config.GetNodesPerGroup(), SlotsPerPage),
                (NodeTableSize / PageSize) * SlotsPerPage) /
            SlotsPerPage;
        // Tombstone key needs to be different from an empty slot key
        memset(Tombstone.Name, 1, NameCapacity - 1);
        Tombstone.NodeId = Max<ui64>();
        Slots = std::make_unique<THt>(
            firstPageNo,
            pageCount,
            PageSize,
            NameSlotSize,
            Tombstone,
            std::move(pageStore),
            [](const TNameTableSlot& s) -> TStringBuf
            { return {s.Name, strlen(s.Name)}; },
            [](const TStringBuf& name) -> ui64
            { return CityHash64(name.data(), name.size()); });

        return pageCount;
    }

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError
    Put(const TString& name, ui64 nodeId, TWriteContext& writeContext)
    {
        if (name.size() >= NameCapacity) {
            return ErrorNameTooLong(name);
        }

        TNameTableSlot slot{};
        memcpy(slot.Name, name.data(), name.size());
        memset(slot.Name + name.size(), 0, NameCapacity - name.size());
        slot.NodeId = nodeId;
        return Slots->Put(writeContext.Lsn, slot, writeContext.PageGroups);
    }

    NProto::TError Delete(const TString& name, TWriteContext& writeContext)
    {
        TNameTableSlot slot{};
        return Slots
            ->Delete(writeContext.Lsn, name, &slot, writeContext.PageGroups);
    }

    NProto::TError Get(const TString& name, ui64* nodeId) const
    {
        ui64 slotNo = 0;
        TNameTableSlot slot{};
        auto error = Slots->Get(0 /* lsn */, name.c_str(), &slot, &slotNo);
        if (HasError(error)) {
            return error;
        }

        *nodeId = slot.NodeId;
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class THandleTable
{
private:
    static constexpr ui64 SlotsPerPage = 256;
    static_assert(SlotsPerPage * HandleSlotSize <= PageSize);

    using THt = TPersistentHashTable<ui64, THandleSlot>;
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 firstPageNo,
        IPageStorePtr pageStore)
    {
        const ui64 maxHandlesPerFile = 10;
        const ui64 pageCount =
            RoundUp(
                maxHandlesPerFile * config.GetNodesPerGroup(),
                SlotsPerPage) /
            SlotsPerPage;
        THandleSlot tombstone{};
        tombstone.Handle = Max<ui64>();
        Slots = std::make_unique<THt>(
            firstPageNo,
            pageCount,
            PageSize,
            HandleSlotSize,
            tombstone,
            std::move(pageStore),
            [](const THandleSlot& s) -> ui64 { return s.Handle; },
            [](const ui64& handle) -> ui64 {
                return CityHash64(
                    reinterpret_cast<const char*>(&handle),
                    sizeof(handle));
            });

        return pageCount;
    }

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError AllocateHandle(ui64* handle)
    {
        while (true) {
            *handle = ShardedId(RandomNumber<ui64>(), 0 /* shardNo */);

            ui64 slotNo = 0;
            THandleSlot slot{};
            auto error = Slots->Get(0 /* lsn */, *handle, &slot, &slotNo);

            if (!HasError(error)) {
                continue;
            }

            if (error.GetCode() == E_FS_NOENT) {
                return {};
            }

            return error;
        }
    }

    NProto::TError Put(THandleSlot v, TWriteContext& writeContext)
    {
        return Slots->Put(writeContext.Lsn, v, writeContext.PageGroups);
    }

    NProto::TError Delete(ui64 handle, TWriteContext& writeContext)
    {
        THandleSlot slot{};
        auto error = Slots->Delete(
            writeContext.Lsn,
            handle,
            &slot,
            writeContext.PageGroups);

        if (error.GetCode() == E_FS_NOENT) {
            error = ErrorInvalidHandle(handle);
        }

        return error;
    }

    NProto::TError Get(ui64 handle, ui64* nodeId) const
    {
        ui64 slotNo = 0;
        THandleSlot slot{};
        auto error = Slots->Get(0 /* lsn */, handle, &slot, &slotNo);
        if (error.GetCode() == E_FS_NOENT) {
            return ErrorInvalidHandle(handle);
        }

        if (HasError(error)) {
            return error;
        }

        *nodeId = slot.NodeId;
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

ui64 CalcPageClusterCount(
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    const ui64 dataPageCount =
        Min(config.GetExpectedGroupCapacity() / PageSize,
            MaxSpacePerStorageGroup / PageSize);
    return RoundUp(dataPageCount, PageClusterPageCount) / PageClusterPageCount;
}

class TPageIndex
{
private:
    static constexpr ui64 SlotsPerPage = 160;
    static_assert(SlotsPerPage * NodePageClusterSlotSize <= PageSize);

    using THt = TPersistentHashTable<TNodePageClusterKey, TNodePageClusterSlot>;
    std::unique_ptr<THt> Slots;

public:
    ui64 Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 firstPageNo,
        IPageStorePtr pageStore)
    {
        const ui64 pageClusterCount = CalcPageClusterCount(config);
        const ui64 indexPageCount =
            RoundUp(pageClusterCount, SlotsPerPage) / SlotsPerPage;
        TNodePageClusterSlot tombstone{};
        tombstone.Key.NodeId = Max<ui64>();
        Slots = std::make_unique<THt>(
            firstPageNo,
            indexPageCount,
            PageSize,
            NodePageClusterSlotSize,
            tombstone,
            std::move(pageStore),
            [](const TNodePageClusterSlot& s) -> TNodePageClusterKey
            { return s.Key; },
            [](const TNodePageClusterKey& k) -> ui64
            {
                return CityHash64(
                    reinterpret_cast<const char*>(&k),
                    sizeof(TNodePageClusterKey));
            });

        return indexPageCount;
    }

    [[nodiscard]] ui64 GetSlotCount() const
    {
        return Slots->GetSlotCount();
    }

    NProto::TError Put(TNodePageClusterSlot v, TWriteContext& writeContext)
    {
        return Slots->Put(writeContext.Lsn, v, writeContext.PageGroups);
    }

    NProto::TError Delete(
        const TNodePageClusterKey& k,
        TWriteContext& writeContext)
    {
        TNodePageClusterSlot slot{};
        return Slots
            ->Delete(writeContext.Lsn, k, &slot, writeContext.PageGroups);
    }

    NProto::TError Get(
        const TNodePageClusterKey& k,
        ui64* storagePageClusterId) const
    {
        ui64 slotNo = 0;
        TNodePageClusterSlot slot{};
        auto error = Slots->Get(0 /* lsn */, k, &slot, &slotNo);
        if (HasError(error)) {
            return error;
        }

        *storagePageClusterId = slot.StoragePageClusterId;
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPageAllocator
{
private:
    std::unique_ptr<TPersistentBitmap> Bitmap;
    ui64 FirstStoragePageClusterId = 0;
    ui64 BitCount = 0;

public:
    ui64 Init(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 firstPageNo,
        IPageStorePtr pageStore)
    {
        const ui64 pageClusterCount = CalcPageClusterCount(config);
        const ui64 bitsPerPage = TPersistentBitmap::CalcBitsPerPage(PageSize);
        BitCount = RoundUp(pageClusterCount, bitsPerPage);
        const ui64 bitmapPageCount = BitCount / bitsPerPage;
        Bitmap = std::make_unique<TPersistentBitmap>(
            firstPageNo,
            bitmapPageCount,
            PageSize,
            std::move(pageStore));
        firstPageNo += bitmapPageCount;
        FirstStoragePageClusterId =
            RoundUp(firstPageNo + bitmapPageCount, PageClusterPageCount) /
            PageClusterPageCount;

        return bitmapPageCount + pageClusterCount * PageClusterPageCount;
    }

    [[nodiscard]] ui64 GetBitCount() const
    {
        return BitCount;
    }

    NProto::TError Allocate(
        ui64 pageClusterCount,
        TVector<ui64>* storagePageClusterIds,
        TWriteContext& writeContext)
    {
        for (ui64 i = 0; i < pageClusterCount; ++i) {
            storagePageClusterIds->push_back(InvalidStoragePageClusterId);
            auto error = Bitmap->Allocate(
                writeContext.Lsn,
                &(*storagePageClusterIds)[i],
                writeContext.PageGroups);
            if (HasError(error)) {
                for (ui64 j = 0; j < i; ++j) {
                    auto error2 = Bitmap->Reset(
                        writeContext.Lsn,
                        (*storagePageClusterIds)[j],
                        writeContext.PageGroups);
                    Y_ABORT_UNLESS(
                        !HasError(error2),
                        "failed to rollback page allocator bitmap changes"
                        ", pageClusterId=%lu, error=%s",
                        (*storagePageClusterIds)[j],
                        FormatError(error2).c_str());
                }

                storagePageClusterIds->clear();
                return error;
            }
        }

        for (ui64& storagePageClusterId: *storagePageClusterIds) {
            storagePageClusterId += FirstStoragePageClusterId;
        }

        return {};
    }

    NProto::TError Deallocate(
        const TVector<ui64>& storagePageClusterIds,
        TWriteContext& writeContext)
    {
        for (ui64 i = 0; i < storagePageClusterIds.size(); ++i) {
            Y_ABORT_UNLESS(
                storagePageClusterIds[i] >= FirstStoragePageClusterId);
            auto error = Bitmap->Reset(
                writeContext.Lsn,
                storagePageClusterIds[i] - FirstStoragePageClusterId,
                writeContext.PageGroups);
            if (HasError(error)) {
                for (ui64 j = 0; j < i; ++j) {
                    auto error2 = Bitmap->Set(
                        writeContext.Lsn,
                        storagePageClusterIds[j],
                        writeContext.PageGroups);
                    Y_ABORT_UNLESS(
                        !HasError(error2),
                        "failed to rollback page allocator bitmap changes"
                        ", pageClusterId=%lu, error=%s",
                        storagePageClusterIds[j],
                        FormatError(error2).c_str());
                }

                return error;
            }
        }

        return {};
    }

    void RollbackAllocation(
        const TVector<ui64>& storagePageClusterIds,
        const TWriteContext& writeContext)
    {
        TVector<TPageGroup> pageGroups;
        for (const ui64 clusterId: storagePageClusterIds) {
            Y_ABORT_UNLESS(clusterId >= FirstStoragePageClusterId);
            auto error = Bitmap->Reset(
                writeContext.Lsn,
                clusterId - FirstStoragePageClusterId,
                pageGroups);

            //
            // Errors aren't expected if allocation has already happened.
            //

            Y_ABORT_UNLESS(!HasError(error));
        }
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
//
// TODO(#5895) - implement layout dump
//

class TFiberShardImpl
{
private:
    const ui32 ShardNo;
    const NProtoPrivate::TPersistentFastShardConfig Config;

    IStorageGroupPtr Storage;
    std::atomic<bool> Acquired = false;
    IPageStorePtr PageStore;
    TNodeTable Nodes;
    TNameTable Names;
    THandleTable Handles;
    TPageIndex PageIndex;
    TPageAllocator PageAllocator;
    mutable silk::FiberMutex Mutex;

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

        TVector<TStorageDevice> devices;
        const auto& sg = Config.GetStorageGroups(0);
        for (const auto& d: sg.GetDevices()) {
            devices.push_back({
                .Node = CreateStorageNodeClient(d.GetHost(), d.GetPort()),
                .DeviceUUID = d.GetDeviceId(),
            });
        }
        Storage = CreateNaiveMirroredStorageGroup(std::move(devices));
        PageStore = CreatePageStore(Storage, PageSize);

        ui64 firstPageNo = 0;
        SILK_INFO("node table offset=%lu", firstPageNo * PageSize);
        const ui64 nodeTablePageCount =
            Nodes.Init(Config, firstPageNo, PageStore);
        firstPageNo += nodeTablePageCount;

        SILK_INFO("name table offset=%lu", firstPageNo * PageSize);
        const ui64 nameTablePageCount =
            Names.Init(Config, firstPageNo, PageStore);
        firstPageNo += nameTablePageCount;

        SILK_INFO("handle table offset=%lu", firstPageNo * PageSize);
        const ui64 handleTablePageCount =
            Handles.Init(Config, firstPageNo, PageStore);
        firstPageNo += handleTablePageCount;

        SILK_INFO("page index offset=%lu", firstPageNo * PageSize);
        const ui64 pageIndexPageCount =
            PageIndex.Init(Config, firstPageNo, PageStore);
        firstPageNo += pageIndexPageCount;

        SILK_INFO("page allocator offset=%lu", firstPageNo * PageSize);
        const ui64 pageAllocatorPageCount =
            PageAllocator.Init(Config, firstPageNo, PageStore);
        firstPageNo += pageAllocatorPageCount;

        SILK_INFO("slack space offset=%lu", firstPageNo * PageSize);

        SILK_INFO("node table slots=%lu", Nodes.GetSlotCount());
        SILK_INFO("name table slots=%lu", Names.GetSlotCount());
        SILK_INFO("handle table slots=%lu", Handles.GetSlotCount());
        SILK_INFO("page index table slots=%lu", PageIndex.GetSlotCount());
        SILK_INFO("page allocator bits=%lu", PageAllocator.GetBitCount());
    }

public:
    NProtoPrivate::TGetNodeAttrBatchResponse GetNodeAttrBatch(
        NProtoPrivate::TGetNodeAttrBatchRequest request)
    {
        NProtoPrivate::TGetNodeAttrBatchResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }

        for (const auto& name: request.GetNames()) {
            NProto::TNodeAttr attr;
            auto error = GetNodeAttr(request.GetNodeId(), name, &attr);
            if (HasError(error)) {
                SILK_DEBUG(
                    "GetNodeAttrBatch::GetNodeAttr error=%s",
                    FormatError(error).c_str());
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
        std::lock_guard g(Mutex);

        if (name) {
            auto error = Names.Get(name, &nodeId);
            if (HasError(error)) {
                return error;
            }
        }

        return Nodes.GetNode(nodeId, attr);
    }

    NProto::TGetNodeAttrResponse GetNodeAttr(
        NProto::TGetNodeAttrRequest request)
    {
        NProto::TGetNodeAttrResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        if (request.GetNodeId() != RootNodeId && !request.GetName().empty()) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }

        NProto::TNodeAttr attr;
        auto error = GetNodeAttr(request.GetNodeId(), request.GetName(), &attr);
        if (HasError(error)) {
            SILK_DEBUG(
                "GetNodeAttr::GetNodeAttr error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
        } else {
            *response.MutableNode() = std::move(attr);
        }

        return response;
    }

    NProto::TSetNodeAttrResponse SetNodeAttr(
        NProto::TSetNodeAttrRequest request)
    {
        NProto::TSetNodeAttrResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        {
            std::lock_guard g(Mutex);

            auto error = Nodes.UpdateNode(
                request.GetNodeId(),
                request.GetFlags(),
                request.GetUpdate(),
                response.MutableNode(),
                writeContext);
            if (HasError(error)) {
                SILK_DEBUG(
                    "SetNodeAttr::Nodes.UpdateNode error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
            }
        }

        auto pages = CollectPages(writeContext);
        auto error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "SetNodeAttr::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            PageStore->RollbackPages(pages);
        } else {
            PageStore->CommitPages(pages);
        }

        return response;
    }

    NProto::TError CreateNodeImpl(
        const TString& name,
        ui32 mode,
        ui64 uid,
        ui64 gid,
        TWriteContext& writeContext,
        NProto::TNodeAttr* attr)
    {
        ui64 nodeId = 0;
        auto error = Nodes.AllocateNodeId(&nodeId);
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateNodeImpl::Nodes.AllocateNodeId error=%s",
                FormatError(error).c_str());
            return error;
        }

        *attr = CreateAttrs(
            ShardedId(nodeId, ShardNo),
            mode,
            0 /* size */,
            uid,
            gid);

        error = Nodes.PutNode(*attr, writeContext);
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateNodeImpl::Nodes.PutNode error=%s",
                FormatError(error).c_str());
            return error;
        }

        return Names.Put(name, attr->GetId(), writeContext);
    }

    NProto::TCreateNodeResponse CreateNode(NProto::TCreateNodeRequest request)
    {
        NProto::TCreateNodeResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

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

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        NProto::TNodeAttr attr;
        NProto::TError error;
        {
            std::lock_guard g(Mutex);
            error = CreateNodeImpl(
                request.GetName(),
                request.GetFile().GetMode(),
                request.GetUid(),
                request.GetGid(),
                writeContext,
                &attr);
        }

        if (HasError(error)) {
            SILK_DEBUG(
                "CreateNode::CreateNodeImpl error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        auto pages = CollectPages(writeContext);
        error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateNode::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            PageStore->RollbackPages(pages);
            return response;
        }

        PageStore->CommitPages(pages);
        *response.MutableNode() = std::move(attr);
        return response;
    }

    NProto::TUnlinkNodeResponse UnlinkNode(NProto::TUnlinkNodeRequest request)
    {
        NProto::TUnlinkNodeResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        if (request.GetNodeId() != RootNodeId) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        //
        // TODO(#5894): take Links into account.
        //

        {
            std::lock_guard g(Mutex);

            ui64 nodeId = 0;
            auto error = Names.Get(request.GetName(), &nodeId);
            if (HasError(error)) {
                SILK_DEBUG(
                    "UnlinkNode::Names.Get error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
                return response;
            }

            error = Names.Delete(request.GetName(), writeContext);
            if (HasError(error)) {
                SILK_DEBUG(
                    "UnlinkNode::Names.Delete error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
                return response;
            }

            error = Nodes.DeleteNode(nodeId, writeContext);
            if (HasError(error)) {
                SILK_DEBUG(
                    "UnlinkNode::Nodes.DeleteNode error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
                return response;
            }

            //
            // TODO(#5894): deallocate pages and delete them from page index.
            //
        }

        auto pages = CollectPages(writeContext);
        auto error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "UnlinkNode::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            PageStore->RollbackPages(pages);
            return response;
        }

        PageStore->CommitPages(pages);
        return response;
    }

    NProto::TCreateHandleResponse CreateHandle(
        NProto::TCreateHandleRequest request)
    {
        NProto::TCreateHandleResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        if (request.GetNodeId() != RootNodeId && !request.GetName().empty()) {
            *response.MutableError() = ErrorInvalidParent(request.GetNodeId());
            return response;
        }

        const ui32 flags = request.GetFlags();
        const auto createFlag = NProto::TCreateHandleRequest::E_CREATE;
        const auto exclFlag = NProto::TCreateHandleRequest::E_EXCLUSIVE;

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        std::unique_lock l(Mutex);

        ui64 nodeId = request.GetNodeId();
        NProto::TNodeAttr attr;
        if (request.GetName().empty()) {
            auto error = Nodes.GetNode(nodeId, &attr);
            if (HasError(error)) {
                SILK_DEBUG(
                    "CreateHandle::Nodes.GetNode error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
            }
        } else {
            auto error = Names.Get(request.GetName(), &nodeId);
            if (error.GetCode() == E_FS_NOENT) {
                if (HasFlag(flags, createFlag)) {
                    auto error = CreateNodeImpl(
                        request.GetName(),
                        request.GetMode(),
                        request.GetUid(),
                        request.GetGid(),
                        writeContext,
                        &attr);
                    if (HasError(error)) {
                        SILK_DEBUG(
                            "CreateHandle::CreateNodeImpl error=%s",
                            FormatError(error).c_str());
                        *response.MutableError() = std::move(error);
                    }

                    nodeId = attr.GetId();
                } else {
                    *response.MutableError() = ErrorInvalidTarget(
                        request.GetNodeId(),
                        request.GetName());
                }
            } else {
                if (HasFlag(flags, createFlag) && HasFlag(flags, exclFlag)) {
                    *response.MutableError() =
                        ErrorAlreadyExists(request.GetName());
                }

                auto error = Nodes.GetNode(nodeId, &attr);
                if (HasError(error)) {
                    SILK_DEBUG(
                        "CreateHandle::Nodes.GetNode error=%s",
                        FormatError(error).c_str());
                    *response.MutableError() = std::move(error);
                }
            }
        }

        if (HasError(response.GetError())) {
            SILK_DEBUG(
                "CreateHandle error=%s",
                FormatError(response.GetError()).c_str());
            return response;
        }

        attr.SetLinks(attr.GetLinks() + 1);
        ui64 handle = 0;
        auto error = Handles.AllocateHandle(&handle);
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateHandle::Handles.AllocateHandle error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        handle = ShardedId(handle, ShardNo);

        error = Handles.Put({.Handle = handle, .NodeId = nodeId}, writeContext);
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateHandle::Handles.Put error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        l.unlock();

        auto pages = CollectPages(writeContext);
        error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "CreateHandle::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            PageStore->RollbackPages(pages);
            return response;
        }

        PageStore->CommitPages(pages);

        response.SetHandle(handle);
        *response.MutableNodeAttr() = std::move(attr);
        return response;
    }

    NProto::TDestroyHandleResponse DestroyHandle(
        NProto::TDestroyHandleRequest request)
    {
        NProto::TDestroyHandleResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        {
            std::lock_guard g(Mutex);

            auto error = Handles.Delete(request.GetHandle(), writeContext);
            if (HasError(error)) {
                SILK_DEBUG(
                    "DestroyHandle::Handles.Delete error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
            }

            //
            // TODO(#5894): update Links.
            //
        }

        if (HasError(response.GetError())) {
            SILK_DEBUG(
                "DestroyHandle error=%s",
                FormatError(response.GetError()).c_str());
            return response;
        }

        auto pages = CollectPages(writeContext);
        auto error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "DestroyHandle::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            PageStore->RollbackPages(pages);
            return response;
        }

        PageStore->CommitPages(pages);

        return response;
    }

    NProto::TWriteDataResponse WriteData(NProto::TWriteDataRequest request)
    {
        NProto::TWriteDataResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        std::unique_lock l(Mutex);

        ui64 nodeId = 0;
        auto error = Handles.Get(request.GetHandle(), &nodeId);
        if (HasError(error)) {
            SILK_DEBUG(
                "WriteData::Handles.Get error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        //
        // Figuring out how many page clusters we need to allocate.
        //

        ui64 pageClusterIdsToAllocate = 0;
        TVector<ui64> storagePageClusterIdsToWrite;

        ui64 bufferOffset = request.GetBufferOffset();
        while (bufferOffset < request.GetBuffer().size()) {
            const ui64 fileOffset =
                bufferOffset - request.GetBufferOffset() + request.GetOffset();
            const ui64 pageClusterId = fileOffset / PageClusterSize;

            ui64 storagePageClusterId = 0;
            error = PageIndex.Get(
                {.NodeId = nodeId, .PageClusterId = pageClusterId},
                &storagePageClusterId);
            if (error.GetCode() == E_FS_NOENT) {
                ++pageClusterIdsToAllocate;
                storagePageClusterIdsToWrite.push_back(
                    InvalidStoragePageClusterId);
                error = {};
            } else if (HasError(error)) {
                SILK_DEBUG(
                    "WriteData::PageIndex.Get error=%s",
                    FormatError(error).c_str());
                break;
            } else {
                storagePageClusterIdsToWrite.push_back(storagePageClusterId);
            }

            const ui64 nextFileOffset =
                RoundDown(fileOffset + PageClusterSize, PageClusterSize);
            bufferOffset += nextFileOffset - fileOffset;
        }

        if (HasError(error)) {
            SILK_DEBUG("WriteData error=%s", FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        TWriteContext writeContext;
        TWriteContextGuard wcg(writeContext, *PageStore);

        //
        // The allocation of all page clusters should happen as a single call.
        // If we do it this way, in the future we can attempt consecutive page
        // allocation for each large write request.
        //

        TVector<ui64> newStoragePageClusterIds;
        if (pageClusterIdsToAllocate) {
            error = PageAllocator.Allocate(
                pageClusterIdsToAllocate,
                &newStoragePageClusterIds,
                writeContext);
            if (HasError(error)) {
                SILK_DEBUG(
                    "WriteData::PageAllocator.Allocate error=%s",
                    FormatError(error).c_str());
                *response.MutableError() = std::move(error);
                return response;
            }

            Y_ABORT_UNLESS(
                newStoragePageClusterIds.size() == pageClusterIdsToAllocate);
        }

        //
        // Updating index and writing pages.
        //

        const ui64 endOffset = request.GetOffset() +
                               request.GetBuffer().size() -
                               request.GetBufferOffset();

        bufferOffset = request.GetBufferOffset();

        auto storagePageClusterIdIt = storagePageClusterIdsToWrite.begin();
        auto newStoragePageClusterIdIt = newStoragePageClusterIds.begin();
        while (bufferOffset < request.GetBuffer().size()) {
            //
            // Updating page index.
            //

            const ui64 fileOffset =
                bufferOffset - request.GetBufferOffset() + request.GetOffset();
            const ui64 pageClusterId = fileOffset / PageClusterSize;

            Y_ABORT_UNLESS(
                storagePageClusterIdIt != storagePageClusterIdsToWrite.end());
            if (*storagePageClusterIdIt == InvalidStoragePageClusterId) {
                Y_ABORT_UNLESS(
                    newStoragePageClusterIdIt !=
                    newStoragePageClusterIds.end());
                *storagePageClusterIdIt = *newStoragePageClusterIdIt;
                ++newStoragePageClusterIdIt;

                TNodePageClusterSlot slot;
                slot.Key.NodeId = nodeId;
                slot.Key.PageClusterId = pageClusterId;
                slot.StoragePageClusterId = *storagePageClusterIdIt;
                error = PageIndex.Put(slot, writeContext);
                if (HasError(error)) {
                    SILK_DEBUG(
                        "WriteData::PageIndex.Put error=%s",
                        FormatError(error).c_str());
                    break;
                }
            }

            //
            // Writing this page cluster to storage.
            //

            const ui64 firstPageInCluster =
                *storagePageClusterIdIt * PageClusterPageCount;

            const ui64 pageClusterOffset = fileOffset % PageClusterSize;
            const ui32 pageNoOffsetInCluster = pageClusterOffset / PageSize;
            ui32 pageNoInCluster = pageNoOffsetInCluster;
            while (pageNoInCluster < PageClusterPageCount) {
                const ui64 storagePageNo = firstPageInCluster + pageNoInCluster;
                const ui64 pageStart =
                    RoundDown(fileOffset, PageSize) +
                    (pageNoInCluster - pageNoOffsetInCluster) * PageSize;
                const ui64 pageEnd = pageStart + PageSize;
                const bool isUnalignedHead =
                    pageNoInCluster == pageNoOffsetInCluster &&
                    pageStart != fileOffset;
                const bool isUnalignedTail = pageEnd > endOffset;

                if (pageStart >= endOffset) {
                    break;
                }

                TString page;
                if (isUnalignedHead || isUnalignedTail) {
                    error = PageStore->ReadPage(
                        writeContext.Lsn,
                        storagePageNo,
                        &page);

                    if (HasError(error)) {
                        SILK_DEBUG(
                            "WriteData::PageStore.ReadPage error=%s",
                            FormatError(error).c_str());
                        break;
                    }
                } else {
                    page.ReserveAndResize(PageSize);
                }

                const ui64 offsetInPage =
                    pageNoInCluster == pageNoOffsetInCluster
                        ? fileOffset - pageStart
                        : 0;
                const ui64 toCopy =
                    Min(pageEnd, endOffset) - (pageStart + offsetInPage);
                memcpy(
                    page.begin() + offsetInPage,
                    request.GetBuffer().data() + bufferOffset,
                    toCopy);

                error = PageStore->WritePage(
                    writeContext.Lsn,
                    storagePageNo,
                    std::move(page),
                    writeContext.PageGroups);

                if (HasError(error)) {
                    SILK_DEBUG(
                        "WriteData::PageStore.WritePage error=%s",
                        FormatError(error).c_str());
                    break;
                }

                bufferOffset += toCopy;
                ++pageNoInCluster;
            }

            if (HasError(error)) {
                SILK_DEBUG("WriteData error=%s", FormatError(error).c_str());
                break;
            }

            ++storagePageClusterIdIt;
        }

        if (HasError(error)) {
            SILK_DEBUG("WriteData error=%s", FormatError(error).c_str());

            PageAllocator.RollbackAllocation(
                newStoragePageClusterIds,
                writeContext);

            *response.MutableError() = std::move(error);
            return response;
        }

        //
        // Write operation is done at this point (but not committed). We should
        // update file size.
        //

        error = Nodes.ResizeNode(nodeId, endOffset, writeContext);

        if (HasError(error)) {
            SILK_DEBUG(
                "WriteData::Nodes.ResizeNode error=%s",
                FormatError(error).c_str());

            PageAllocator.RollbackAllocation(
                newStoragePageClusterIds,
                writeContext);

            *response.MutableError() = std::move(error);
            return response;
        }

        l.unlock();

        //
        // Everything's fine, time to commit the result.
        //

        auto pages = CollectPages(writeContext);
        error = Storage->WriteLogRecord(
            std::move(writeContext.Headers),
            std::move(writeContext.PageGroups));
        if (HasError(error)) {
            SILK_DEBUG(
                "WriteData::WriteLogRecord error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);

            //
            // We should first rollback the allocation - we should do it while
            // the bitmap pages are still marked as dirty at our lsn. And only
            // after that we can rollback the pages.
            //

            l.lock();
            PageAllocator.RollbackAllocation(
                newStoragePageClusterIds,
                writeContext);
            l.unlock();

            PageStore->RollbackPages(pages);

            return response;
        }

        PageStore->CommitPages(pages);

        return response;
    }

    NProto::TReadDataResponse ReadData(NProto::TReadDataRequest request)
    {
        NProto::TReadDataResponse response;
        if (!AcquireIfNeeded(response)) {
            return response;
        }

        std::lock_guard l(Mutex);

        ui64 nodeId = 0;
        auto error = Handles.Get(request.GetHandle(), &nodeId);
        if (HasError(error)) {
            SILK_DEBUG(
                "ReadData::Handles.Get error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return response;
        }

        auto& buffer = *response.MutableBuffer();
        buffer.resize(request.GetLength(), 0);
        const ui64 endOffset = request.GetOffset() + request.GetLength();

        ui64 bufferOffset = 0;

        while (bufferOffset < buffer.size()) {
            //
            // Reading page index.
            //

            const ui64 fileOffset = bufferOffset + request.GetOffset();
            const ui64 pageClusterId = fileOffset / PageClusterSize;
            const ui64 pageClusterOffset = fileOffset % PageClusterSize;

            ui64 storagePageClusterId = 0;
            error = PageIndex.Get(
                {.NodeId = nodeId, .PageClusterId = pageClusterId},
                &storagePageClusterId);
            if (error.GetCode() == E_FS_NOENT) {
                //
                // Page cluster mapping not found - filling the corresponding
                // part of the buffer with zeroes.
                //

                const ui64 toSet =
                    Min(PageClusterSize - pageClusterOffset,
                        buffer.size() - bufferOffset);
                memset(buffer.begin() + bufferOffset, 0, toSet);
                bufferOffset += toSet;
                error = {};
                continue;
            }

            if (HasError(error)) {
                SILK_DEBUG(
                    "ReadData::PageIndex.Get error=%s",
                    FormatError(error).c_str());
                break;
            }

            //
            // Page cluster mapping found - reading the pages into the buffer.
            //

            const ui64 firstPageInCluster =
                storagePageClusterId * PageClusterPageCount;

            const ui32 pageNoOffsetInCluster = pageClusterOffset / PageSize;
            ui32 pageNoInCluster = pageNoOffsetInCluster;
            while (pageNoInCluster < PageClusterPageCount) {
                const ui64 storagePageNo = firstPageInCluster + pageNoInCluster;
                const ui64 pageStart =
                    RoundDown(fileOffset, PageSize) +
                    (pageNoInCluster - pageNoOffsetInCluster) * PageSize;
                const ui64 pageEnd = pageStart + PageSize;

                if (pageStart >= endOffset) {
                    break;
                }

                TString page;
                error = PageStore->ReadPage(0 /* lsn */, storagePageNo, &page);

                if (HasError(error)) {
                    SILK_DEBUG(
                        "ReadData::PageStore.ReadPage error=%s",
                        FormatError(error).c_str());
                    break;
                }

                const ui64 offsetInPage =
                    pageNoInCluster == pageNoOffsetInCluster
                        ? fileOffset - pageStart
                        : 0;
                const ui64 toCopy =
                    Min(pageEnd, endOffset) - (pageStart + offsetInPage);
                memcpy(
                    buffer.begin() + bufferOffset,
                    page.begin() + offsetInPage,
                    toCopy);

                bufferOffset += toCopy;
                ++pageNoInCluster;
            }

            if (HasError(error)) {
                SILK_DEBUG("ReadData error=%s", FormatError(error).c_str());
                break;
            }
        }

        if (HasError(error)) {
            SILK_DEBUG("ReadData error=%s", FormatError(error).c_str());
            buffer.clear();
            *response.MutableError() = std::move(error);
        }

        return response;
    }

    //
    // Access/Allocate API is deliberately no-op in prototype.
    //

    NProto::TAccessNodeResponse AccessNode(NProto::TAccessNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAllocateDataResponse AllocateData(
        NProto::TAllocateDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    //
    // Ok to keep locks API unsupported in prototype.
    //

    NProto::TAcquireLockResponse AcquireLock(
        NProto::TAcquireLockRequest request)
    {
        return NotImplemented<NProto::TAcquireLockResponse>(std::move(request));
    }

    NProto::TReleaseLockResponse ReleaseLock(
        NProto::TReleaseLockRequest request)
    {
        return NotImplemented<NProto::TReleaseLockResponse>(std::move(request));
    }

    NProto::TTestLockResponse TestLock(NProto::TTestLockRequest request)
    {
        return NotImplemented<NProto::TTestLockResponse>(std::move(request));
    }

    //
    // Ok to keep xattr API unsupported in prototype.
    //

    NProto::TRemoveNodeXAttrResponse RemoveNodeXAttr(
        NProto::TRemoveNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(request);
    }

    NProto::TGetNodeXAttrResponse GetNodeXAttr(
        NProto::TGetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(request);
    }

    NProto::TSetNodeXAttrResponse SetNodeXAttr(
        NProto::TSetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(request);
    }

    NProto::TListNodeXAttrResponse ListNodeXAttr(
        NProto::TListNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(request);
    }

private:
    template <typename TResponse, typename TRequest>
    TResponse NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_FS_NOTSUPP);
        return response;
    }

    template <typename TResponse>
    bool AcquireIfNeeded(TResponse& response)
    {
        if (Acquired) {
            return true;
        }

        std::lock_guard g(Mutex);
        if (Acquired) {
            return true;
        }

        auto error = Storage->AcquireDevices();
        if (HasError(error)) {
            SILK_DEBUG(
                "AcquireIfNeeded::Storage.AcquireDevices error=%s",
                FormatError(error).c_str());
            *response.MutableError() = std::move(error);
            return false;
        }

        Acquired = true;
        return true;
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
    {}

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
                .Request = std::make_shared<ns::T##name##Request>(             \
                    std::move(request)),                                       \
                .Promise = promise,                                            \
            },                                                                 \
            nullptr /* future */);                                             \
        if (r) {                                                               \
            ns::T##name##Response response;                                    \
            *response.MutableError() = MakeError(                              \
                E_FAIL,                                                        \
                TStringBuilder()                                               \
                    << "failed to spawn fiber: " << ::strerror(r));            \
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
