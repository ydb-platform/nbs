#include "server.h"

#include "backend_aio.h"
#include "backend_rdma.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryptor.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service/storage_test.h>
#include <cloud/blockstore/libs/service_local/compound_storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>

#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/condvar.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/tempfile.h>
#include <util/system/thread.h>

#include <vhost/blockdev.h>

#include <array>
#include <atomic>
#include <cstring>
#include <span>
#include <thread>

IOutputStream& operator<<(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EEncryptionMode mode)
{
    const auto& s = NCloud::NBlockStore::NProto::EEncryptionMode_Name(mode);
    Y_DEBUG_ABORT_UNLESS(s);
    out << s;

    return out;
}

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NVHostServer {


using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui8 GetFillChar(size_t block)
{
    constexpr char AllowedChars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    return AllowedChars[block % strlen(AllowedChars)];
}

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey("1234567890123456789012345678901");

using TBlocksPerRequest = size_t;
using TBlockSize = ui32;
using TUnaligned = bool;
using TThreadCount = size_t;
using TTestParams = std::tuple<
    NProto::EEncryptionMode,
    TBlocksPerRequest,
    TBlockSize,
    TUnaligned,
    TThreadCount>;

class TMockEncryptor: public IEncryptor
{
public:
    enum class EBehaviour
    {
        ReturnError,
        EncryptToAllZeroes,
    };

private:
    EBehaviour Behaviour;

public:
    explicit TMockEncryptor(EBehaviour behaviour)
        : Behaviour(behaviour)
    {}

    NProto::TError Encrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(blockIndex);
        switch (Behaviour) {
            case EBehaviour::ReturnError: {
                return MakeError(E_FAIL, "Oh no!");
            }
            case EBehaviour::EncryptToAllZeroes:{
                memset(const_cast<char*>(dst.Data()), 0, dst.Size());
                return {};
            }
        }
    }

    NProto::TError Decrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(dst);
        Y_UNUSED(blockIndex);
        return MakeError(E_NOT_IMPLEMENTED);
    }
};

class TServerTest
    : public testing::TestWithParam<TTestParams>
{
public:
    static constexpr ui32 QueueCount = 8;
    static constexpr ui32 QueueIndex = 4;
    static constexpr ui64 ChunkCount = 3;
    static constexpr ui64 ChunkByteCount = 128_KB;
    static constexpr ui64 TotalByteCount = ChunkByteCount * ChunkCount;
    static constexpr ui64 SectorSize = VHD_SECTOR_SIZE;
    static constexpr ui64 TotalSectorCount = TotalByteCount / SectorSize;

    static constexpr i64 HeaderSize = 4_KB;
    static constexpr i64 PaddingSize = 1_KB;

    const NProto::EEncryptionMode EncryptionMode = std::get<0>(GetParam());
    const size_t BlocksPerRequest = std::get<1>(GetParam());
    const ui32 BlockSize = std::get<2>(GetParam());
    const size_t ThreadCount = std::get<4>(GetParam());
    const ui64 BlocksPerChunk = ChunkByteCount / BlockSize;
    const ui32 SectorsPerBlock = BlockSize / SectorSize;
    const size_t SectorsPerRequest = SectorsPerBlock * BlocksPerRequest;
    const ui64 TotalBlockCount = TotalByteCount / BlockSize;
    const ui64 RequestSize = BlockSize * BlocksPerRequest;
    const bool Unaligned = std::get<3>(GetParam());
    const TString SocketPath = "server_ut.vhost";
    const TString Serial = "server_ut";

    NCloud::ILoggingServicePtr Logging;
    std::shared_ptr<IServer> Server;
    TVector<TTempFileHandle> Files;
    IEncryptorPtr Encryptor;

    TOptions Options {
        .SocketPath = SocketPath,
        .Serial = Serial,
        .NoSync = true,
        .NoChmod = true,
        .BlockSize = BlockSize,
        .QueueCount = QueueCount,
    };

    NVHost::TClient Client {SocketPath, { .QueueCount = QueueCount }};

    TMonotonicBufferResource Memory;

public:
    TServerTest()
    {
        if (EncryptionMode == NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
            Encryptor =
                CreateAesXtsEncryptor(TEncryptionKey(DefaultEncryptionKey));
        }
    }

    void StartServer(bool addNonExistingDevice = false)
    {
        Server = CreateServer(
            Logging,
            CreateAioBackend(Encryptor, Logging, ThreadCount));

        Options.Layout.reserve(ChunkCount);
        Files.reserve(ChunkCount);

        if (addNonExistingDevice) {
            Options.Layout.push_back(
                {.DevicePath = "NonExistingDevice",
                 .ByteCount = ChunkByteCount});
        }

        for (ui32 i = 0; i != ChunkCount - addNonExistingDevice; ++i) {
            auto& file = Files.emplace_back(MakeTempName());

            Options.Layout.push_back({
                .DevicePath = file.GetName(),
                .ByteCount = ChunkByteCount
            });

            file.Resize(ChunkByteCount);
        }

        Server->Start(Options);

        ASSERT_TRUE(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void StartServerWithSplitDevices()
    {
        Server = CreateServer(
            Logging,
            CreateAioBackend(Encryptor, Logging, ThreadCount));

        // H - header
        // D - device
        // P - padding
        //
        // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]

        TVector<ui64> offsets(ChunkCount);
        std::generate_n(
            offsets.begin(),
            ChunkCount,
            [&, offset = HeaderSize] () mutable {
                return std::exchange(offset, offset + PaddingSize + ChunkByteCount);
            });

        std::swap(offsets.front(), offsets.back());

        auto& file = Files.emplace_back(MakeTempName());

        const size_t fileSize = HeaderSize
            + ChunkCount * ChunkByteCount
            + PaddingSize * (ChunkCount - 1);

        file.Resize(fileSize);

        // fill the header
        {
            char header[HeaderSize];
            std::memset(header, 'H', HeaderSize);
            file.Pwrite(header, HeaderSize, 0);
        }

        // fill the space between devices
        {
            char padding[PaddingSize];
            std::memset(padding, 'P', PaddingSize);

            i64 offset = HeaderSize + ChunkByteCount;
            for (ui32 i = 0; i != ChunkCount - 1; ++i) {
                file.Pwrite(padding, PaddingSize, offset);

                offset += PaddingSize + ChunkByteCount;
            }
        }

        Options.Layout.reserve(ChunkCount);

        for (ui32 i = 0; i != ChunkCount; ++i) {
            Options.Layout.push_back({
                .DevicePath = file.GetName(),
                .ByteCount = ChunkByteCount,
                .Offset = offsets[i]
            });
        }

        Server->Start(Options);

        ASSERT_TRUE(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void SetUp() override
    {
        ASSERT_GE(BlockSize, SectorSize);
        Logging = NCloud::CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_DEBUG});
    }

    void TearDown() override
    {
        if (Server) {
            Client.DeInit();
            Server->Stop();
            Server.reset();
        }
        Files.clear();
        Options.Layout.clear();
    }

    TCompleteStats GetStats(ui64 expectedCompleted) const
    {
        return GetStats(
            [expectedCompleted](const TCompleteStats& stats)
            { return stats.SimpleStats.Completed == expectedCompleted; });
    }

    TCompleteStats GetStats(
        std::function<bool(const TCompleteStats&)> func) const
    {
        // Without I/O, stats are synced every second and only if there is a
        // pending GetStats call. The first call to GetStats might not bring the
        // latest stats; therefore, you need at least two calls so that the AIO
        // backend will sync the stats.

        TSimpleStats prevStats;
        TCompleteStats stats;
        for (int i = 0; i != 5; ++i) {
            // Save critical events from previous attempt.
            auto critEvents = std::move(stats.CriticalEvents);

            stats = Server->GetStats(prevStats);

            // Combine critical events from previous and current attempt.
            if (critEvents) {
                for (auto& critEvent: stats.CriticalEvents) {
                    critEvents.push_back(std::move(critEvent));
                }
                stats.CriticalEvents = std::move(critEvents);
            }

            // Check that the current attempt to get statistics has brought
            // everything we need.
            if (func(stats)) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        return stats;
    }

    TString LoadRawBlock(ui64 block)
    {
        const ui64 chunkIndex = block / BlocksPerChunk;
        const auto& chunkLayout = Options.Layout[chunkIndex];

        auto it = FindIf(
            Files,
            [&](const TFile& f)
            { return f.GetName() == chunkLayout.DevicePath; });
        if (it == Files.end()) {
            return "File " + chunkLayout.DevicePath + " not found";
        }
        auto & file = *it;

        const ui64 fileOffset =
            chunkLayout.Offset + (block % BlocksPerChunk) * BlockSize;

        TString buffer;
        buffer.resize(BlockSize);
        file.Seek(fileOffset, SeekDir::sSet);
        file.Load(&buffer[0], buffer.size());

        return buffer;
    }

    TString LoadBlockAndDecrypt(ui64 block)
    {
        TString buffer = LoadRawBlock(block);
        if (Encryptor && !IsAllZeroes(buffer.data(), buffer.size())) {
            Y_DEBUG_ABORT_UNLESS(buffer.size() % SectorSize == 0);
            for (ui32 i = 0; i < SectorsPerBlock; ++i) {
                Encryptor->Decrypt(
                    TBlockDataRef(buffer.data() + (i * SectorSize), SectorSize),
                    TBlockDataRef(buffer.data() + (i * SectorSize), SectorSize),
                    i + block * SectorsPerBlock);
            }
        }
        return buffer;
    }

    bool SaveRawBlock(ui64 block, const TString& data)
    {
        const ui64 chunkIndex = block / BlocksPerChunk;
        const auto& chunkLayout = Options.Layout[chunkIndex];

        auto it = FindIf(
            Files,
            [&](const TFile& f)
            { return f.GetName() == chunkLayout.DevicePath; });
        if (it == Files.end()) {
            return false;
        }
        auto & file = *it;

        const ui64 fileOffset =
            chunkLayout.Offset + (block % BlocksPerChunk) * BlockSize;

        if (data.size() != BlockSize) {
            return false;
        }
        file.Seek(fileOffset, SeekDir::sSet);
        file.Write(&data[0], data.size());

        return true;
    }

    TString MakePattern(size_t startBlock) const
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename ... Ts>
std::span<char> Create(TMonotonicBufferResource& mem, Ts&& ... args)
{
    std::span buf = mem.Allocate(sizeof(T), alignof(T));
    if (buf.empty()) {
        return {};
    }

    new (buf.data()) T {std::forward<Ts>(args)...};

    return buf;
}

auto Hdr(TMonotonicBufferResource& mem, virtio_blk_req_hdr hdr)
{
    return Create<virtio_blk_req_hdr>(mem, hdr);
}

TString MakeRandomPattern(size_t size) {
    TString result;
    result.resize(size);

    for (size_t i = 0; i < size; ++i) {
        result[i] = RandomNumber<ui8>(255);
    }
    return result;
}

class TAioIoSizeTest: public TServerTest
{
protected:
    void SendFailedRead(ui64 firstSector)
    {
        auto header =
            Hdr(Memory, {.type = VIRTIO_BLK_T_IN, .sector = firstSector});
        auto data =
            Memory.Allocate(RequestSize + (Unaligned ? 1 : 0), BlockSize);
        auto buffer = data.subspan(Unaligned ? 1 : 0, RequestSize);
        auto status = Memory.Allocate(1);
        auto result = Client.WriteAsync(QueueIndex, {header}, {buffer, status});
        ASSERT_TRUE(result.Wait(TDuration::Seconds(10)));
        ASSERT_EQ(RequestSize + 1, result.GetValueSync());
        ASSERT_EQ(VIRTIO_BLK_S_IOERR, status[0]);
    }
};

class TTestRdmaStorageProvider final: public IStorageProvider
{
private:
    const IStoragePtr Storage;

public:
    explicit TTestRdmaStorageProvider(IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    NThreading::TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId, NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);

        return NThreading::MakeFuture<IStoragePtr>(Storage);
    }
};

class TTestRdmaCompletionStats final: public ICompletionStats
{
private:
    TMutex Mutex;
    TCondVar Changed;
    TSimpleStats Snapshot;

public:
    std::optional<TSimpleStats> Get(TDuration timeout) override
    {
        Y_UNUSED(timeout);

        TGuard<TMutex> guard(Mutex);
        return Snapshot;
    }

    void Sync(const TSimpleStats& stats) override
    {
        {
            TGuard<TMutex> guard(Mutex);
            Snapshot = stats;
        }

        Changed.BroadCast();
    }

    void Sync(const TAtomicStats& stats) override
    {
        TSimpleStats snapshot;
        snapshot += stats;
        Sync(snapshot);
    }

    std::optional<TSimpleStats> WaitForCompleted(ui64 expectedCompleted)
    {
        TGuard<TMutex> guard(Mutex);

        const bool ready = Changed.WaitT(
            Mutex,
            TDuration::Seconds(10),
            [&] { return Snapshot.Completed >= expectedCompleted; });

        if (!ready) {
            return std::nullopt;
        }

        return Snapshot;
    }
};

enum class ETestRdmaResult
{
    Success,
    Error,
    RetryThenSuccess,
    RetryThenError,
};

class TRdmaServerTest: public TServerTest
{
public:
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<TTestRdmaCompletionStats> CompletionStats;

    std::atomic<ui32> ReadAttempts = 0;
    std::atomic<ui32> WriteAttempts = 0;

    std::atomic<ui64> ReadBytesSeen = 0;
    std::atomic<ui64> WriteBytesSeen = 0;
    std::atomic<ui64> ReadStartSeen = 0;
    std::atomic<ui64> WriteStartSeen = 0;

    ETestRdmaResult Result = ETestRdmaResult::Success;
    ui64 SentRequestCount = 0;

    NProto::TError MakeAttemptError(ui32 attempt) const
    {
        const bool shouldRetry = Result == ETestRdmaResult::RetryThenSuccess ||
                                 Result == ETestRdmaResult::RetryThenError;

        if (shouldRetry && attempt == 0) {
            return MakeError(E_REJECTED, "test retry");
        }

        if (Result == ETestRdmaResult::Error ||
            Result == ETestRdmaResult::RetryThenError)
        {
            return MakeError(E_FAIL, "test final error");
        }

        return {};
    }

    void StartRdmaServer(ETestRdmaResult result)
    {
        Result = result;

        Storage = std::make_shared<TTestStorage>();
        Storage->DoAllocations = true;

        Storage->ReadBlocksLocalHandler = [this](auto callContext, auto request)
        {
            Y_UNUSED(callContext);
            EXPECT_EQ(BlockSize, request->GetBlockSize());

            ReadBytesSeen.store(
                static_cast<ui64>(request->GetBlocksCount()) *
                request->GetBlockSize());
            ReadStartSeen.store(request->GetStartIndex());

            const ui32 attempt = ReadAttempts.fetch_add(1);

            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() = MakeAttemptError(attempt);

            auto guard = request->Sglist.Acquire();
            EXPECT_TRUE(guard);
            if (guard) {
                EXPECT_EQ(ReadBytesSeen.load(), SgListGetSize(guard.Get()));
                if (!HasError(response)) {
                    for (const auto& buffer: guard.Get()) {
                        std::memset(
                            const_cast<char*>(buffer.Data()),
                            'r', buffer.Size());
                    }
                }
            }

            return NThreading::MakeFuture(std::move(response));
        };

        Storage->WriteBlocksLocalHandler =
            [this](auto callContext, auto request)
        {
            Y_UNUSED(callContext);
            EXPECT_EQ(BlockSize, request->GetBlockSize());

            WriteBytesSeen.store(
                static_cast<ui64>(request->BlocksCount) *
                request->GetBlockSize());
            WriteStartSeen.store(request->GetStartIndex());

            const ui32 attempt = WriteAttempts.fetch_add(1);

            NProto::TWriteBlocksLocalResponse response;
            *response.MutableError() = MakeAttemptError(attempt);

            auto guard = request->Sglist.Acquire();
            EXPECT_TRUE(guard);
            if (guard) {
                EXPECT_EQ(WriteBytesSeen.load(), SgListGetSize(guard.Get()));
                for (const auto& buffer: guard.Get()) {
                    EXPECT_EQ(
                        TString(buffer.Size(), 'x'),
                        TString(buffer.Data(), buffer.Size()));
                }
            }

            return NThreading::MakeFuture(std::move(response));
        };

        Options.DeviceBackend = "rdma";
        Options.Layout = {
            {
                .DevicePath = "rdma://127.0.0.1:10020/test-device",
                .ByteCount = TotalByteCount,
                .Offset = 0,
            },
        };

        ASSERT_NO_FATAL_FAILURE(StartRdmaServerWithStorage(Storage));
    }

    void StartRdmaServerWithStorage(IStoragePtr storage)
    {
        CompletionStats = std::make_shared<TTestRdmaCompletionStats>();
        Options.DeviceBackend = "rdma";

        auto provider =
            std::make_shared<TTestRdmaStorageProvider>(std::move(storage));

        Server = CreateServer(
            Logging,
            CreateRdmaBackend(Logging, std::move(provider), CompletionStats));

        Server->Start(Options);

        ASSERT_TRUE(Client.Init());

        Memory = TMonotonicBufferResource{Client.GetMemory()};
    }

    void SendRequest(
        bool read,
        bool expectSuccess,
        bool severalBuffers = false,
        ui64 requestBytes = 0, ui64 firstSector = 0)
    {
        if (!requestBytes) {
            requestBytes = RequestSize;
        }

        auto header =
            Hdr(Memory,
                {
                    .type = static_cast<ui32>(
                        read ? VIRTIO_BLK_T_IN : VIRTIO_BLK_T_OUT),
                    .sector = firstSector,
                });

        ASSERT_FALSE(header.empty());

        const size_t extraByte = Unaligned ? 1 : 0;
        auto allocation = Memory.Allocate(requestBytes + extraByte, BlockSize);

        ASSERT_EQ(requestBytes + extraByte, allocation.size());

        // Shift the buffer address by one byte for an unaligned request.
        auto data = allocation.subspan(extraByte, requestBytes);
        std::memset(data.data(), 'x', data.size());

        auto status = Memory.Allocate(1);
        ASSERT_EQ(1u, status.size());
        status[0] = static_cast<char>(0xff);

        NVHost::TSgList inBuffers{header};
        NVHost::TSgList outBuffers;

        auto& dataBuffers = read ? outBuffers : inBuffers;

        if (severalBuffers) {
            const size_t middle = data.size() / 2;
            dataBuffers.push_back(data.first(middle));
            dataBuffers.push_back(data.subspan(middle));
        } else {
            dataBuffers.push_back(data);
        }

        outBuffers.push_back(status);

        auto operation = Client.WriteAsync(QueueIndex, inBuffers, outBuffers);

        ASSERT_TRUE(operation.Wait(TDuration::Seconds(10)));

        const ui32 responseLength = operation.GetValue();

        EXPECT_EQ(
            read ? requestBytes + status.size() : status.size(),
            responseLength);
        EXPECT_EQ(
            expectSuccess ? VIRTIO_BLK_S_OK : VIRTIO_BLK_S_IOERR,
            static_cast<ui8>(status[0]));

        if (read && expectSuccess) {
            EXPECT_EQ(
                TString(requestBytes, 'r'), TString(data.data(), data.size()));
        } else if (read && !ReadAttempts.load()) {
            EXPECT_EQ(
                TString(requestBytes, 'x'), TString(data.data(), data.size()));
        }

        ++SentRequestCount;
        const auto snapshot =
            CompletionStats->WaitForCompleted(SentRequestCount);

        ASSERT_TRUE(snapshot.has_value());
    }

    void CheckCounters(
        bool expectSuccess,
        ui32 expectedAttempts, ui64 requestBytes = 0, ui64 firstSector = 0)
    {
        if (!requestBytes) {
            requestBytes = RequestSize;
        }

        const auto snapshot = CompletionStats->WaitForCompleted(2);

        ASSERT_TRUE(snapshot.has_value());

        // In every scenario we send one read and one write.
        EXPECT_EQ(2u, snapshot->Completed);

        const ui64 expectedCount = expectSuccess ? 1 : 0;
        const ui64 expectedBytes = expectSuccess ? requestBytes : 0;
        const ui64 expectedErrors = expectSuccess ? 0 : 1;

        for (const auto type: {VHD_BDEV_READ, VHD_BDEV_WRITE}) {
            const auto& request = snapshot->Requests[type];

            EXPECT_EQ(expectedCount, request.Count);
            EXPECT_EQ(expectedBytes, request.Bytes);
            EXPECT_EQ(expectedErrors, request.Errors);

            EXPECT_EQ(expectedCount, request.IoSizeCount);
            EXPECT_EQ(expectedBytes, request.IoSizeBytes);

            EXPECT_EQ(request.Count, request.IoSizeCount);
        }

        EXPECT_EQ(expectedAttempts, ReadAttempts.load());
        EXPECT_EQ(expectedAttempts, WriteAttempts.load());

        EXPECT_EQ(requestBytes, ReadBytesSeen.load());
        EXPECT_EQ(requestBytes, WriteBytesSeen.load());
        EXPECT_EQ(firstSector / SectorsPerBlock, ReadStartSeen.load());
        EXPECT_EQ(firstSector / SectorsPerBlock, WriteStartSeen.load());
    }
};

class TRdmaCompoundServerTest: public TRdmaServerTest
{
private:
    struct TPendingPart
    {
        ui32 StorageIndex = 0;
        ui64 StartIndex = 0;
        ui32 BlocksCount = 0;
        ui32 RequestBlockSize = 0;
        ui64 SgBytes = 0;
        TString WriteData;
        std::function<void(NProto::TError)> Complete;
    };

    TMutex PartsMutex;
    TCondVar PartsChanged;
    TVector<TPendingPart> Parts;
    std::array<std::shared_ptr<TTestStorage>, 2> Storages;

    void AddPart(TPendingPart part)
    {
        auto completed = std::make_shared<std::atomic<bool>>(false);
        part.Complete =
            [complete = std::move(part.Complete), completed](auto error)
        {
            if (!completed->exchange(true)) {
                complete(std::move(error));
            }
        };

        {
            TGuard<TMutex> guard(PartsMutex);
            Parts.push_back(std::move(part));
        }
        PartsChanged.BroadCast();
    }

    std::optional<std::array<TPendingPart, 2>> WaitForParts(size_t offset)
    {
        TGuard<TMutex> guard(PartsMutex);
        if (!PartsChanged.WaitT(
                PartsMutex,
                TDuration::Seconds(10),
                [&] { return Parts.size() >= offset + 2; }))
        {
            return std::nullopt;
        }

        return std::array<TPendingPart, 2>{Parts[offset], Parts[offset + 1]};
    }

public:
    void TearDown() override
    {
        TVector<TPendingPart> parts;
        {
            TGuard<TMutex> guard(PartsMutex);
            parts = Parts;
        }
        for (const auto& part: parts) {
            part.Complete(MakeError(E_FAIL, "cancel unfinished test part"));
        }

        TRdmaServerTest::TearDown();
    }

    void StartCompoundRdmaServer()
    {
        for (ui32 storageIndex = 0; storageIndex != Storages.size();
             ++storageIndex)
        {
            auto& storage = Storages[storageIndex];
            storage = std::make_shared<TTestStorage>();
            storage->DoAllocations = true;

            storage->ReadBlocksLocalHandler =
                [this, storageIndex](auto callContext, auto request)
            {
                Y_UNUSED(callContext);

                auto promise =
                    NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
                auto guard = request->Sglist.Acquire();
                EXPECT_TRUE(guard);

                AddPart({
                    .StorageIndex = storageIndex,
                    .StartIndex = request->GetStartIndex(),
                    .BlocksCount = request->GetBlocksCount(),
                    .RequestBlockSize = request->GetBlockSize(),
                    .SgBytes = guard ? SgListGetSize(guard.Get()) : 0,
                    .Complete =
                        [promise, request, storageIndex](auto error) mutable
                    {
                        if (!HasError(error)) {
                            auto guard = request->Sglist.Acquire();
                            EXPECT_TRUE(guard);
                            if (guard) {
                                for (const auto& buffer: guard.Get()) {
                                    std::memset(
                                        const_cast<char*>(buffer.Data()),
                                        'a' + storageIndex, buffer.Size());
                                }
                            }
                        }
                        NProto::TReadBlocksLocalResponse response;
                        *response.MutableError() = std::move(error);
                        promise.SetValue(std::move(response));
                    },
                });

                return promise.GetFuture();
            };

            storage->WriteBlocksLocalHandler =
                [this, storageIndex](auto callContext, auto request)
            {
                Y_UNUSED(callContext);

                auto promise =
                    NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
                auto guard = request->Sglist.Acquire();
                EXPECT_TRUE(guard);

                TString data;
                if (guard) {
                    data.resize(SgListGetSize(guard.Get()));
                    EXPECT_EQ(
                        data.size(),
                        SgListCopy(guard.Get(), {data.begin(), data.size()}));
                }

                AddPart({
                    .StorageIndex = storageIndex,
                    .StartIndex = request->GetStartIndex(),
                    .BlocksCount = request->BlocksCount,
                    .RequestBlockSize = request->GetBlockSize(),
                    .SgBytes = data.size(),
                    .WriteData = std::move(data),
                    .Complete =
                        [promise](auto error) mutable
                    {
                        NProto::TWriteBlocksLocalResponse response;
                        *response.MutableError() = std::move(error);
                        promise.SetValue(std::move(response));
                    },
                });

                return promise.GetFuture();
            };
        }

        Options.Layout = {
            {
                .DevicePath = "rdma://127.0.0.1:10020/first-device",
                .ByteCount = ChunkByteCount,
            },
            {
                .DevicePath = "rdma://127.0.0.1:10020/second-device",
                .ByteCount = ChunkByteCount,
            },
        };

        auto storage = NServer::CreateCompoundStorage(
            {Storages[0], Storages[1]},
            {BlocksPerChunk, 2 * BlocksPerChunk},
            BlockSize,
            {},
            {}, CreateServerStatsStub());

        ASSERT_NO_FATAL_FAILURE(StartRdmaServerWithStorage(std::move(storage)));
    }

    void SendCompoundRequest(
        bool read, ETestRdmaResult result, ui32 firstCompletedPart)
    {
        const ui64 requestBytes = 2 * BlockSize;
        auto header =
            Hdr(Memory,
                {
                    .type = static_cast<ui32>(
                        read ? VIRTIO_BLK_T_IN : VIRTIO_BLK_T_OUT),
                    .sector = (BlocksPerChunk - 1) * SectorsPerBlock,
                });
        ASSERT_FALSE(header.empty());
        auto allocation =
            Memory.Allocate(requestBytes + (Unaligned ? 1 : 0), BlockSize);
        ASSERT_EQ(requestBytes + (Unaligned ? 1 : 0), allocation.size());
        auto data = allocation.subspan(Unaligned ? 1 : 0, requestBytes);
        std::memset(data.data(), 'a', BlockSize);
        std::memset(data.data() + BlockSize, 'b', BlockSize);
        if (read) {
            std::memset(data.data(), 'x', data.size());
        }
        auto status = Memory.Allocate(1);
        ASSERT_EQ(1u, status.size());
        status[0] = static_cast<char>(0xff);

        size_t partsOffset = 0;
        {
            TGuard<TMutex> guard(PartsMutex);
            partsOffset = Parts.size();
        }

        // A single data buffer still produces two real storage requests.
        auto operation =
            read ? Client.WriteAsync(QueueIndex, {header}, {data, status})
                 : Client.WriteAsync(QueueIndex, {header, data}, {status});

        const bool retry = result == ETestRdmaResult::RetryThenSuccess ||
                           result == ETestRdmaResult::RetryThenError;
        const bool success = result == ETestRdmaResult::Success ||
                             result == ETestRdmaResult::RetryThenSuccess;

        for (ui32 attempt = 0; attempt != (retry ? 2u : 1u); ++attempt) {
            const auto parts = WaitForParts(partsOffset + 2 * attempt);
            ASSERT_TRUE(parts.has_value());

            for (ui32 storageIndex = 0; storageIndex != 2; ++storageIndex) {
                const auto& part = (*parts)[storageIndex];
                EXPECT_EQ(storageIndex, part.StorageIndex);
                EXPECT_EQ(
                    storageIndex ? 0u : BlocksPerChunk - 1, part.StartIndex);
                EXPECT_EQ(1u, part.BlocksCount);
                EXPECT_EQ(BlockSize, part.RequestBlockSize);
                EXPECT_EQ(BlockSize, part.SgBytes);
                if (!read) {
                    EXPECT_EQ(
                        TString(BlockSize, 'a' + storageIndex), part.WriteData);
                }
            }

            NProto::TError error;
            if (retry && attempt == 0) {
                error = MakeError(E_REJECTED, "retry one compound part");
            } else if (!success) {
                error = MakeError(E_FAIL, "fail one compound part");
            }

            // Only one child fails. Exercise both orders of its completion.
            (*parts)[firstCompletedPart].Complete(
                firstCompletedPart == 0 ? error : NProto::TError{});
            EXPECT_FALSE(operation.HasValue());
            const auto snapshot = CompletionStats->Get(TDuration::Zero());
            ASSERT_TRUE(snapshot.has_value());
            EXPECT_EQ(SentRequestCount, snapshot->Completed);
            const auto& pendingCounters =
                snapshot->Requests[read ? VHD_BDEV_READ : VHD_BDEV_WRITE];
            EXPECT_EQ(0u, pendingCounters.Count);
            EXPECT_EQ(0u, pendingCounters.IoSizeCount);
            EXPECT_EQ(0u, pendingCounters.IoSizeBytes);

            (*parts)[1 - firstCompletedPart].Complete(
                firstCompletedPart == 1 ? error : NProto::TError{});
        }

        ASSERT_TRUE(operation.Wait(TDuration::Seconds(10)));
        EXPECT_EQ(
            read ? requestBytes + status.size() : status.size(),
            operation.GetValue());
        EXPECT_EQ(
            success ? VIRTIO_BLK_S_OK : VIRTIO_BLK_S_IOERR,
            static_cast<ui8>(status[0]));
        if (read && success) {
            EXPECT_EQ(
                TString(BlockSize, 'a') + TString(BlockSize, 'b'),
                TString(data.data(), data.size()));
        }

        ++SentRequestCount;
        const auto snapshot =
            CompletionStats->WaitForCompleted(SentRequestCount);
        ASSERT_TRUE(snapshot.has_value());
        EXPECT_EQ(SentRequestCount, snapshot->Completed);

        const auto& counters =
            snapshot->Requests[read ? VHD_BDEV_READ : VHD_BDEV_WRITE];
        const ui64 expectedCount = success ? 1 : 0;
        EXPECT_EQ(expectedCount, counters.Count);
        EXPECT_EQ(success ? requestBytes : 0, counters.Bytes);
        EXPECT_EQ(success ? 0u : 1u, counters.Errors);
        EXPECT_EQ(expectedCount, counters.IoSizeCount);
        EXPECT_EQ(success ? requestBytes : 0, counters.IoSizeBytes);

        TGuard<TMutex> guard(PartsMutex);
        EXPECT_EQ(partsOffset + (retry ? 4 : 2), Parts.size());
    }
};

class TRdmaSectorServerTest: public TRdmaServerTest
{
};

class TRdmaBlockAlignmentTest: public TRdmaServerTest
{
};

class TConcurrentRdmaCompletionStats final: public ICompletionStats
{
private:
    TTestRdmaCompletionStats Impl;
    std::atomic<ui32> ActivePublishers = 0;

public:
    std::atomic<bool> ConcurrentPublication = false;

    std::optional<TSimpleStats> Get(TDuration timeout) override
    {
        return Impl.Get(timeout);
    }

    void Sync(const TSimpleStats& stats) override
    {
        if (ActivePublishers.fetch_add(1) != 0) {
            ConcurrentPublication = true;
        }

        // Widen the publication window without protecting the backend's data.
        // The backend must serialize writers before invoking this interface.
        std::this_thread::yield();
        Impl.Sync(stats);
        ActivePublishers.fetch_sub(1);
    }

    void Sync(const TAtomicStats& stats) override
    {
        TSimpleStats snapshot;
        snapshot += stats;
        Sync(snapshot);
    }
};

class TRdmaConcurrentServerTest: public testing::TestWithParam<ui32>
{
private:
    static constexpr ui32 BlockSize = 4096;
    static constexpr ui32 Rounds = 32;
    static constexpr ui32 RequestsPerRound = 4;

    const ui32 QueueCount = GetParam();
    const TString SocketPath = "concurrent_rdma_server_ut.vhost";
    NVHost::TClient Client{SocketPath, {.QueueCount = QueueCount}};
    TMonotonicBufferResource Memory;
    std::shared_ptr<IServer> Server;

    struct TRequest
    {
        bool Read = false;
        bool Success = false;
        std::span<char> Header;
        std::span<char> Data;
        std::span<char> Status;
    };

public:
    void TearDown() override
    {
        if (Server) {
            Client.DeInit();
            Server->Stop();
        }
    }

    void RunConcurrentRequests()
    {
        auto logging = NCloud::CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_CRIT});
        auto storage = std::make_shared<TTestStorage>();
        auto completionStats =
            std::make_shared<TConcurrentRdmaCompletionStats>();
        std::atomic<ui64> readAttempts = 0;
        std::atomic<ui64> writeAttempts = 0;

        TMutex pendingMutex;
        TCondVar pendingChanged;
        TVector<std::function<void()>> pending;
        bool stopCompletions = false;

        const auto enqueueCompletion = [&](std::function<void()> complete)
        {
            {
                TGuard<TMutex> guard(pendingMutex);
                pending.push_back(std::move(complete));
            }
            pendingChanged.Signal();
        };

        storage->ReadBlocksLocalHandler = [&](auto callContext, auto request)
        {
            Y_UNUSED(callContext);
            ++readAttempts;
            EXPECT_EQ(BlockSize, request->GetBlockSize());
            EXPECT_EQ(1u, request->GetBlocksCount());
            EXPECT_EQ(0u, request->GetStartIndex());
            auto promise =
                NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
            enqueueCompletion(
                [promise, request]() mutable
                {
                    auto guard = request->Sglist.Acquire();
                    EXPECT_TRUE(guard);
                    if (guard) {
                        EXPECT_EQ(BlockSize, SgListGetSize(guard.Get()));
                        for (const auto& buffer: guard.Get()) {
                            std::memset(
                                const_cast<char*>(buffer.Data()),
                                'r', buffer.Size());
                        }
                    }
                    promise.SetValue(NProto::TReadBlocksLocalResponse{});
                });
            return promise.GetFuture();
        };
        storage->WriteBlocksLocalHandler = [&](auto callContext, auto request)
        {
            Y_UNUSED(callContext);
            ++writeAttempts;
            EXPECT_EQ(BlockSize, request->GetBlockSize());
            EXPECT_EQ(1u, request->BlocksCount);
            EXPECT_EQ(0u, request->GetStartIndex());
            auto promise =
                NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
            enqueueCompletion(
                [promise, request]() mutable
                {
                    auto guard = request->Sglist.Acquire();
                    EXPECT_TRUE(guard);
                    if (guard) {
                        EXPECT_EQ(BlockSize, SgListGetSize(guard.Get()));
                        for (const auto& buffer: guard.Get()) {
                            EXPECT_EQ(
                                TString(buffer.Size(), 'x'),
                                TString(buffer.Data(), buffer.Size()));
                        }
                    }
                    promise.SetValue(NProto::TWriteBlocksLocalResponse{});
                });
            return promise.GetFuture();
        };

        auto provider = std::make_shared<TTestRdmaStorageProvider>(storage);
        Server = CreateServer(
            logging, CreateRdmaBackend(logging, provider, completionStats));
        TOptions options{
            .SocketPath = SocketPath,
            .Serial = "concurrent_rdma_server_ut",
            .NoSync = true,
            .NoChmod = true,
            .BlockSize = BlockSize,
            .QueueCount = QueueCount,
        };
        options.DeviceBackend = "rdma";
        options.Layout = {{
            .DevicePath = "rdma://127.0.0.1:10020/test-device",
            .ByteCount = 128_KB,
        }};
        Server->Start(options);
        ASSERT_TRUE(Client.Init());
        Memory = TMonotonicBufferResource{Client.GetMemory()};

        // Allocate in the main thread. Each producer then owns one queue and
        // separate buffers, reused only after every request in its round ends.
        TVector<std::array<TRequest, RequestsPerRound>> requests(QueueCount);
        for (auto& queue: requests) {
            for (ui32 i = 0; i != RequestsPerRound; ++i) {
                auto& request = queue[i];
                request.Read = i % 2 == 0;
                request.Success = i < 2;
                const ui64 bytes = !request.Success && request.Read
                                       ? VHD_SECTOR_SIZE
                                       : BlockSize;
                request.Header = Hdr(
                    Memory,
                    {
                        .type = static_cast<ui32>(
                            request.Read ? VIRTIO_BLK_T_IN : VIRTIO_BLK_T_OUT),
                        .sector = !request.Success && !request.Read ? 1u : 0u,
                    });
                request.Data = Memory.Allocate(bytes, BlockSize);
                request.Status = Memory.Allocate(1);
                ASSERT_FALSE(request.Header.empty());
                ASSERT_EQ(bytes, request.Data.size());
                ASSERT_EQ(1u, request.Status.size());
            }
        }

        std::atomic<bool> start = false;
        std::atomic<bool> stopReader = false;
        std::atomic<ui64> snapshotsRead = 0;

        // Successful requests complete on this worker, independently of the
        // queue threads rejecting short lengths and misaligned offsets.
        std::thread completer(
            [&]
            {
                for (;;) {
                    std::function<void()> complete;
                    {
                        TGuard<TMutex> guard(pendingMutex);
                        pendingChanged.WaitI(
                            pendingMutex,
                            [&]
                            { return stopCompletions || !pending.empty(); });
                        if (pending.empty()) {
                            break;
                        }
                        complete = std::move(pending.back());
                        pending.pop_back();
                    }
                    complete();
                    std::this_thread::yield();
                }
            });
        std::thread reader(
            [&]
            {
                while (!start.load()) {
                    std::this_thread::yield();
                }
                ui64 previousCompleted = 0;
                do {
                    const auto snapshot =
                        completionStats->Get(TDuration::Zero());
                    EXPECT_TRUE(snapshot.has_value());
                    if (snapshot) {
                        EXPECT_LE(previousCompleted, snapshot->Completed);
                        previousCompleted = snapshot->Completed;
                        ui64 completed = 0;
                        for (const auto type: {VHD_BDEV_READ, VHD_BDEV_WRITE}) {
                            const auto& counters = snapshot->Requests[type];
                            EXPECT_EQ(counters.Count, counters.IoSizeCount);
                            EXPECT_EQ(
                                counters.Count * BlockSize,
                                counters.IoSizeBytes);
                            EXPECT_EQ(counters.IoSizeBytes, counters.Bytes);
                            ui64 sizesCount = 0;
                            snapshot->Sizes[type].IterateBuckets(
                                [&](ui64, ui64, ui64 count)
                                { sizesCount += count; });
                            ui64 timesCount = 0;
                            snapshot->Times[type].IterateBuckets(
                                [&](ui64, ui64, ui64 count)
                                { timesCount += count; });
                            EXPECT_EQ(counters.Count, sizesCount);
                            EXPECT_EQ(counters.Count, timesCount);
                            completed += counters.Count + counters.Errors;
                        }
                        EXPECT_EQ(completed, snapshot->Completed);
                        ++snapshotsRead;
                    }
                    std::this_thread::yield();
                } while (!stopReader.load());
            });
        TVector<std::thread> producers;
        for (ui32 queueIndex = 0; queueIndex != QueueCount; ++queueIndex) {
            producers.emplace_back(
                [&, queueIndex]
                {
                    while (!start.load()) {
                        std::this_thread::yield();
                    }
                    for (ui32 round = 0; round != Rounds; ++round) {
                        std::array<NThreading::TFuture<ui32>, RequestsPerRound>
                            operations;
                        for (ui32 i = 0; i != RequestsPerRound; ++i) {
                            auto& request = requests[queueIndex][i];
                            std::memset(
                                request.Data.data(), 'x', request.Data.size());
                            request.Status[0] = static_cast<char>(0xff);
                            operations[i] =
                                request.Read
                                    ? Client.WriteAsync(
                                          queueIndex,
                                          {request.Header},
                                          {request.Data, request.Status})
                                    : Client.WriteAsync(
                                          queueIndex,
                                          {request.Header, request.Data},
                                          {request.Status});
                        }
                        for (ui32 i = 0; i != RequestsPerRound; ++i) {
                            const auto& request = requests[queueIndex][i];
                            const bool ready =
                                operations[i].Wait(TDuration::Seconds(10));
                            EXPECT_TRUE(ready);
                            if (!ready) {
                                return;
                            }
                            EXPECT_EQ(
                                request.Read ? request.Data.size() + 1 : 1,
                                operations[i].GetValue());
                            EXPECT_EQ(
                                request.Success ? VIRTIO_BLK_S_OK
                                                : VIRTIO_BLK_S_IOERR,
                                static_cast<ui8>(request.Status[0]));
                            if (request.Read) {
                                EXPECT_EQ(
                                    TString(
                                        request.Data.size(),
                                        request.Success ? 'r' : 'x'),
                                    TString(
                                        request.Data.data(),
                                        request.Data.size()));
                            }
                        }
                    }
                });
        }
        start = true;
        for (auto& producer: producers) {
            producer.join();
        }

        // Keep the completion worker and handler captures alive until all
        // queues have stopped, including requests left pending after a timeout.
        Client.DeInit();
        Server->Stop();
        Server.reset();

        {
            TGuard<TMutex> guard(pendingMutex);
            stopCompletions = true;
        }
        pendingChanged.Signal();
        completer.join();
        stopReader = true;
        reader.join();

        EXPECT_GT(snapshotsRead.load(), 0u);
        EXPECT_FALSE(completionStats->ConcurrentPublication.load());
        const auto snapshot = completionStats->Get(TDuration::Zero());
        ASSERT_TRUE(snapshot.has_value());
        const ui64 requestsPerType = static_cast<ui64>(QueueCount) * Rounds;
        EXPECT_EQ(RequestsPerRound * requestsPerType, snapshot->Completed);
        EXPECT_EQ(requestsPerType, readAttempts.load());
        EXPECT_EQ(requestsPerType, writeAttempts.load());
        for (const auto type: {VHD_BDEV_READ, VHD_BDEV_WRITE}) {
            const auto& counters = snapshot->Requests[type];
            EXPECT_EQ(requestsPerType, counters.Count);
            EXPECT_EQ(requestsPerType, counters.Errors);
            EXPECT_EQ(requestsPerType * BlockSize, counters.Bytes);
            EXPECT_EQ(requestsPerType, counters.IoSizeCount);
            EXPECT_EQ(requestsPerType * BlockSize, counters.IoSizeBytes);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST_P(TRdmaServerTest, ShouldCountLogicalReadAndWriteBytes)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Success));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, true));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, true));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(true, 1));
}

TEST_P(TRdmaServerTest, ShouldIgnoreFailedRequestsInIoSize)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Error));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, false));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, false));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(false, 1));
}

TEST_P(TRdmaServerTest, ShouldCountRetriedRequestOnce)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::RetryThenSuccess));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, true));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, true));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(true, 2));
}

TEST_P(TRdmaServerTest, ShouldIgnoreRetryThenFinalErrorInIoSize)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::RetryThenError));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, false));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, false));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(false, 2));
}

TEST_P(TRdmaServerTest, ShouldCountOneRequestForSeveralBuffers)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Success));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, true, true));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, true, true));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(true, 1));
}

INSTANTIATE_TEST_SUITE_P(
    IoSize,
    TRdmaServerTest,
    testing::Combine(
        testing::Values(NProto::NO_ENCRYPTION),
        testing::Values(size_t{1}, size_t{16}),
        testing::Values(ui32{512}, ui32{4096}),
        testing::Values(false, true), testing::Values(size_t{1})));

TEST_P(TRdmaCompoundServerTest, ShouldCountOneParentForTwoStorageCompletions)
{
    ASSERT_NO_FATAL_FAILURE(StartCompoundRdmaServer());
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(true, ETestRdmaResult::Success, 0));
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(false, ETestRdmaResult::Success, 1));
}

TEST_P(TRdmaCompoundServerTest, ShouldIgnoreParentWhenOneStoragePartFails)
{
    ASSERT_NO_FATAL_FAILURE(StartCompoundRdmaServer());
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(true, ETestRdmaResult::Error, 0));
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(false, ETestRdmaResult::Error, 1));
}

TEST_P(TRdmaCompoundServerTest, ShouldCountRetriedCompoundParentOnce)
{
    ASSERT_NO_FATAL_FAILURE(StartCompoundRdmaServer());
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(true, ETestRdmaResult::RetryThenSuccess, 0));
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(false, ETestRdmaResult::RetryThenSuccess, 1));
}

TEST_P(TRdmaCompoundServerTest, ShouldIgnoreRetryThenFailedCompoundParent)
{
    ASSERT_NO_FATAL_FAILURE(StartCompoundRdmaServer());
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(true, ETestRdmaResult::RetryThenError, 0));
    ASSERT_NO_FATAL_FAILURE(
        SendCompoundRequest(false, ETestRdmaResult::RetryThenError, 1));
}

INSTANTIATE_TEST_SUITE_P(
    IoSize,
    TRdmaCompoundServerTest,
    testing::Combine(
        testing::Values(NProto::NO_ENCRYPTION),
        testing::Values(size_t{2}),
        testing::Values(ui32{512}, ui32{4096}),
        testing::Values(false, true), testing::Values(size_t{1})));

TEST_P(TRdmaSectorServerTest, ShouldCountSectorSizedIoAtNonzeroOffset)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Success));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, true, false, SectorSize, 1));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, true, false, SectorSize, 1));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(true, 1, SectorSize, 1));
}

TEST_P(TRdmaSectorServerTest, ShouldCountSubPageIoAtNonzeroOffset)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Success));

    ASSERT_NO_FATAL_FAILURE(SendRequest(true, true, false, 3 * SectorSize, 3));
    ASSERT_NO_FATAL_FAILURE(SendRequest(false, true, false, 3 * SectorSize, 3));

    ASSERT_NO_FATAL_FAILURE(CheckCounters(true, 1, 3 * SectorSize, 3));
}

INSTANTIATE_TEST_SUITE_P(
    IoSize,
    TRdmaSectorServerTest,
    testing::Combine(
        testing::Values(NProto::NO_ENCRYPTION),
        testing::Values(size_t{16}),
        testing::Values(ui32{512}),
        testing::Values(false, true), testing::Values(size_t{1})));

TEST_P(TRdmaBlockAlignmentTest, ShouldRejectUnalignedLengthOrOffsetBeforeIo)
{
    ASSERT_NO_FATAL_FAILURE(StartRdmaServer(ETestRdmaResult::Success));

    const std::array<std::pair<ui64, ui64>, 3> requests = {{
        {SectorSize, 0},
        {BlockSize + SectorSize, 0},
        {BlockSize, 1},
    }};

    for (const auto& [requestBytes, firstSector]: requests) {
        ASSERT_NO_FATAL_FAILURE(
            SendRequest(true, false, false, requestBytes, firstSector));
        ASSERT_NO_FATAL_FAILURE(
            SendRequest(false, false, false, requestBytes, firstSector));

        const auto snapshot =
            CompletionStats->WaitForCompleted(SentRequestCount);
        ASSERT_TRUE(snapshot.has_value());
        EXPECT_EQ(SentRequestCount, snapshot->Completed);

        for (const auto type: {VHD_BDEV_READ, VHD_BDEV_WRITE}) {
            const auto& counters = snapshot->Requests[type];
            EXPECT_EQ(0u, counters.Count);
            EXPECT_EQ(0u, counters.Bytes);
            EXPECT_EQ(SentRequestCount / 2, counters.Errors);
            EXPECT_EQ(0u, counters.IoSizeCount);
            EXPECT_EQ(0u, counters.IoSizeBytes);
        }

        EXPECT_EQ(0u, ReadAttempts.load());
        EXPECT_EQ(0u, WriteAttempts.load());
        EXPECT_EQ(0u, ReadBytesSeen.load());
        EXPECT_EQ(0u, WriteBytesSeen.load());
    }
}

INSTANTIATE_TEST_SUITE_P(
    IoSize,
    TRdmaBlockAlignmentTest,
    testing::Combine(
        testing::Values(NProto::NO_ENCRYPTION),
        testing::Values(size_t{1}),
        testing::Values(ui32{4096}),
        testing::Values(false, true), testing::Values(size_t{1})));

TEST_P(
    TRdmaConcurrentServerTest,
    ShouldSerializeRejectedRequestsAndAsyncCompletionsWithSnapshots)
{
    ASSERT_NO_FATAL_FAILURE(RunConcurrentRequests());
}

INSTANTIATE_TEST_SUITE_P(
    IoSize, TRdmaConcurrentServerTest, testing::Values(ui32{1}, ui32{8}));

TEST_P(TServerTest, ShouldGetDeviceID)
{
    StartServer();

    std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_GET_ID});
    std::span serial = Memory.Allocate(VIRTIO_BLK_DISKID_LENGTH);
    std::span status = Memory.Allocate(1);

    const ui32 len =
        Client.WriteAsync(QueueIndex, {hdr}, {serial, status}).GetValueSync();

    EXPECT_EQ(serial.size() + status.size(), len);
    EXPECT_EQ(Serial, TStringBuf(serial.data()));
    EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
}

TEST_P(TAioIoSizeTest, ShouldTrackIoSizeForFailedSingleAio)
{
    StartServer();
    Files[0].Resize(0);
    SendFailedRead(0);
    const auto stats = GetStats(1).SimpleStats;
    ASSERT_EQ(1u, stats.Completed);
    const auto& read = stats.Requests[0];
    EXPECT_EQ(0u, read.Count);
    EXPECT_EQ(0u, read.IoSizeCount);
    EXPECT_EQ(0u, read.IoSizeBytes);
    EXPECT_EQ(1u, read.Errors);
    EXPECT_EQ(RequestSize, read.Bytes);
}

TEST_P(TAioIoSizeTest, ShouldTrackIoSizeForFailedCompoundAio)
{
    StartServer();
    // Both parts fail, independently of their completion order.
    Files[0].Resize(0);
    Files[1].Resize(0);
    SendFailedRead(ChunkByteCount / SectorSize - SectorsPerBlock);
    const auto stats = GetStats(2).SimpleStats;
    ASSERT_EQ(2u, stats.Completed);
    const auto& read = stats.Requests[0];
    // A failed parent is excluded after aggregating all device results.
    EXPECT_EQ(0u, read.Count);
    EXPECT_EQ(0u, read.IoSizeCount);
    EXPECT_EQ(0u, read.IoSizeBytes);
    EXPECT_EQ(1u, read.Errors);
    EXPECT_EQ(RequestSize, read.Bytes);
}

INSTANTIATE_TEST_SUITE_P(
    IoSize,
    TAioIoSizeTest,
    testing::Combine(
        testing::Values(NProto::EEncryptionMode::NO_ENCRYPTION),
        testing::Values(2),
        testing::Values(512, 4096),
        testing::Values(false, true), testing::Values(0)));

TEST_P(TServerTest, ShouldReadAndWrite)
{
    StartServer();

    // write data
    size_t writesCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer = Memory.Allocate(
            RequestSize,
            Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
            TString expectedData = MakePattern(i);
            memcpy(
                writeBuffer.data(),
                expectedData.data(),
                expectedData.size());
            auto writeOp =
                Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
            EXPECT_EQ(status.size(), writeOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
            ++writesCount;
        }
    }

    // read data
    size_t readsCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
            auto readOp =
                Client.WriteAsync(QueueIndex, {hdr}, {readBuffer, status});
            EXPECT_EQ(status.size() + readBuffer.size(), readOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);

            TString readData(readBuffer.data(), readBuffer.size());
            EXPECT_EQ(MakePattern(i), readData);
            ++readsCount;
        }
    }

    // validate storage
    for (ui64 i = 0; i != TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, GetFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writesCount + readsCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readsCount, read.Count);
        EXPECT_EQ(readsCount, read.IoSizeCount);
        EXPECT_EQ(readsCount * RequestSize, read.IoSizeBytes);
        EXPECT_EQ(readsCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readsCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writesCount, write.Count);
        EXPECT_EQ(writesCount, write.IoSizeCount);
        EXPECT_EQ(writesCount * RequestSize, write.IoSizeBytes);
        EXPECT_EQ(writesCount * RequestSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(
            Unaligned ? writesCount - splittedWrites : 0,
            write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldResponseWithEIOOnRequestsToNonExistingDevice)
{
    StartServer(true);

    // write data
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
            0 * SectorsPerBlock;
        TString expectedData = MakePattern(0);
        memcpy(writeBuffer.data(), expectedData.data(), expectedData.size());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
        EXPECT_EQ(status.size(), writeOp.GetValueSync());
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, status[0]);
    }
}

TEST_P(TServerTest, ShouldWriteToSplitDevices)
{
    // The test does not depend on this value.
    if (BlocksPerRequest != 1) {
        return;
    }

    StartServerWithSplitDevices();

    TVector<char> blocksFill(TotalBlockCount);

    // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]
    auto verifyLayoutAndData = [&]()
    {
        char header[HeaderSize];
        TFile file{Files[0].GetName(), EOpenModeFlag::OpenAlways};

        // Check header
        file.Load(header, HeaderSize);
        EXPECT_EQ(HeaderSize, std::count(header, header + HeaderSize, 'H'));

        const TString paddingData(PaddingSize, 'P');
        // Check paddings
        for (ui32 i = 0; i != ChunkCount; ++i) {
            // Skip blocks data
            file.Seek(ChunkByteCount, SeekDir::sCur);

            // Check padding
            if (i + 1 != ChunkCount) {
                TString realPadding(PaddingSize, 0);
                file.Load(const_cast<char*>(realPadding.data()), PaddingSize);
                EXPECT_EQ(paddingData, realPadding);
            }
        }

        // Check blocks
        for (ui32 i = 0; i < TotalBlockCount; ++i) {
            const TString expectedData(BlockSize, blocksFill[i]);
            const TString realData = LoadBlockAndDecrypt(i);
            EXPECT_EQ(expectedData, realData);
        }
    };
    // initial verification
    verifyLayoutAndData();

    // disk:   [ --- Dn-1 --- | --- D1 --- | ... | --- Dn-2 --- | --- D0 --- ]
    // write:        ^------------^
    //           offset: ChunkByteCount/2
    //           size:   ChunkByteCount
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span buffer =
            Memory.Allocate(ChunkByteCount, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        const ui64 startBlock = ChunkByteCount / 2 / BlockSize;
        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
            startBlock * SectorsPerBlock;

        for (ui32 i = 0; i < BlocksPerChunk; ++i) {
            const char blockFill = 'A' + (startBlock + i) % 26;
            blocksFill[startBlock + i] = blockFill;
            std::memset(buffer.data() + i * BlockSize, blockFill, BlockSize);
        }

        auto writeOp = Client.WriteAsync(QueueIndex, {hdr, buffer}, {status});
        const ui32 len = writeOp.GetValueSync();

        EXPECT_EQ(status.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
    }

    // verification after cross-chunk write
    verifyLayoutAndData();
}

TEST_P(TServerTest, ShouldHandleMultipleQueues)
{
    if (!Unaligned) {
        // TODO fix data-race
        return;
    }
    StartServer();

    TVector<char> blocksFill(TotalBlockCount);
    const ui32 requestCount = 10;

    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;

    for (ui64 i = 0; i != requestCount; ++i) {
        ui64 startBlock = (i * BlocksPerRequest) % TotalBlockCount;
        std::span hdr = Hdr(
            Memory,
            {.type = VIRTIO_BLK_T_OUT, .sector = startBlock * SectorsPerBlock});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        EXPECT_EQ(RequestSize, writeBuffer.size());
        EXPECT_EQ(1u, status.size());

        ui8 bufferFill = 'A' + i % 26;
        memset(writeBuffer.data(), bufferFill, writeBuffer.size_bytes());
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            blocksFill[startBlock + j] = bufferFill;
        }

        statuses.push_back(status);
        futures.push_back(
            Client.WriteAsync(i % QueueCount, {hdr, writeBuffer}, {status}));
    }

    WaitAll(futures).Wait();

    const auto completeStats = GetStats(requestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(requestCount, stats.Submitted);
    EXPECT_EQ(requestCount, stats.Completed);
    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);

    for (ui32 i = 0; i != requestCount; ++i) {
        const ui32 len = futures[i].GetValueSync();

        EXPECT_EQ(statuses[i].size(), len);
        EXPECT_EQ(char(0), statuses[i][0]);
    }

    // Check blocks
    for (ui32 i = 0; i < TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, blocksFill[i]);
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }
}

TEST_P(TServerTest, ShouldWriteMultipleAndReadByOne)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; i++) {
        const TString pattern = MakeRandomPattern(writeBuffer.size_bytes());

        // write BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
        writeCount++;

        // read one block at a time
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                (i + j) * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);
            std::string_view readData(readBuffer.data(), readBuffer.size());
            std::string_view expectedData(
                pattern.data() + BlockSize * j,
                BlockSize);
            EXPECT_EQ(expectedData, readData);
            ++readCount;
        }
    }

    // validate stats
    const auto splittedReads = 0;
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * BlockSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * BlocksPerRequest * BlockSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldWriteByOneAndReadMultiple)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;

    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        const TString pattern = MakeRandomPattern(RequestSize);

        // write one block at a time
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                (i + j) * SectorsPerBlock;
            memcpy(
                writeBuffer.data(),
                pattern.data() + BlockSize * j,
                writeBuffer.size_bytes());
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(writeStatus.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
            ++writeCount;
        }

        // read BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
            i * SectorsPerBlock;
        auto read_result =
            Client.WriteAsync(QueueIndex, {hdr_r}, {readBuffer, readStatus});
        const ui32 len = read_result.GetValueSync();
        EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);
        std::string_view readData(readBuffer.data(), readBuffer.size());
        std::string_view expectedData(pattern.data(), readBuffer.size());
        EXPECT_EQ(expectedData, readData);
        ++readCount;
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = 0;
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * BlockSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldHandleWrongSectorIndex)
{
    StartServer();

    {
        std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span writeStatus = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            TotalSectorCount;
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
    }

    {
        std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span readStatus = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
            TotalSectorCount;
        auto read_result =
            Client.WriteAsync(QueueIndex, {hdr_r}, {readBuffer, readStatus});
        const ui32 len = read_result.GetValueSync();
        EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, readStatus[0]);
    }
    // validate stats
    const auto completeStats = GetStats(0);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(0u, stats.Completed);
    EXPECT_EQ(0u, stats.Dequeued);
    EXPECT_EQ(0u, stats.Submitted);
}

TEST_P(TServerTest, ShouldStoreEncryptedZeroes)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);
    const auto pattern = TString(writeBuffer.size_bytes(), 0);

    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        // write BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);

        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            const auto rawBlock = LoadRawBlock(i + j);
            const bool shouldReadZeroes =
                EncryptionMode == NProto::EEncryptionMode::NO_ENCRYPTION;
            EXPECT_EQ(
                shouldReadZeroes,
                IsAllZeroes(rawBlock.data(), rawBlock.size()));
        }
    }
}

TEST_P(TServerTest, ShouldReadAndWriteMultipleBuffers)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer1 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeBuffer2 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer1 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readBuffer2 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);


    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest * 2; ++i) {
        const TString pattern1 = MakeRandomPattern(RequestSize);
        const TString pattern2 = MakeRandomPattern(RequestSize);

        // check writes
        {
            memcpy(writeBuffer1.data(), pattern1.data(), writeBuffer1.size_bytes());
            memcpy(writeBuffer2.data(), pattern2.data(), writeBuffer2.size_bytes());

            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                i * SectorsPerBlock;
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer1, writeBuffer2},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(1u, len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
        }

        // check reads
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                i * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer1, readBuffer2, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(
                readStatus.size() + readBuffer1.size() + readBuffer2.size(),
                len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);

            TString readData1(readBuffer1.data(), readBuffer1.size());
            EXPECT_EQ(pattern1, readData1);
            TString readData2(readBuffer2.data(), readBuffer2.size());
            EXPECT_EQ(pattern2, readData2);
        }
    }
}

TEST_P(TServerTest, ShouldStatEncryptorErrors)
{
    if (EncryptionMode != NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
        return;
    }

    Encryptor = std::make_shared<TMockEncryptor>(
        TMockEncryptor::EBehaviour::ReturnError);
    StartServer();

    // Fill storage with random data
    {
        TString randomBlock = MakeRandomPattern(BlockSize);
        for (size_t i = 0; i < TotalBlockCount; ++i) {
            ASSERT_TRUE(SaveRawBlock(i, randomBlock));
        }
    }

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        // check writes
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                i * SectorsPerBlock;
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(1u, len);
            EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
            ++writeCount;
        }

        // check reads
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                i * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_IOERR, readStatus[0]);
            ++readCount;
        }
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto completeStats = GetStats(readCount + splittedReads);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(readCount + splittedReads, stats.Completed);
    EXPECT_EQ(readCount + splittedReads, stats.Dequeued);
    EXPECT_EQ(readCount + splittedReads, stats.Submitted);
    EXPECT_EQ(readCount + writeCount, stats.EncryptorErrors);
    // Decryption fails after AIO has already updated Count.
    EXPECT_EQ(readCount, stats.Requests[0].Count);
    EXPECT_EQ(readCount, stats.Requests[0].IoSizeCount);
    EXPECT_EQ(readCount * RequestSize, stats.Requests[0].IoSizeBytes);
    // Encryption fails before submitting any AIO write.
    EXPECT_EQ(0u, stats.Requests[1].Count);
    EXPECT_EQ(0u, stats.Requests[1].IoSizeCount);
    EXPECT_EQ(0u, stats.Requests[1].IoSizeBytes);
}

TEST_P(TServerTest, ShouldStatAllZeroesBlocks)
{
    if (EncryptionMode != NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
        return;
    }

    Encryptor = std::make_shared<TMockEncryptor>(
        TMockEncryptor::EBehaviour::EncryptToAllZeroes);
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(1u, len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
        ++writeCount;
    }

    // validate stats
    const auto completeStats = GetStats(
        [](const TCompleteStats& stats) {
            return stats.CriticalEvents.size() != 0 &&
                   stats.SimpleStats.EncryptorErrors != 0;
        });
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(0u, stats.Completed);
    EXPECT_EQ(0u, stats.Dequeued);
    EXPECT_EQ(0u, stats.Submitted);
    EXPECT_EQ(writeCount, stats.EncryptorErrors);

    // validate crit events
    EXPECT_EQ(writeCount, completeStats.CriticalEvents.size());
    for (const auto& [sensorName, message]: completeStats.CriticalEvents) {
        EXPECT_EQ("EncryptorGeneratedZeroBlock", sensorName);
        EXPECT_EQ(
            true,
            message.StartsWith("Encryptor has generated a zero block #"));
    }
}

TEST_P(TServerTest, ShouldReadAndWriteWithPteFlushEnabled)
{
    Options.PteFlushByteThreshold = 4096;
    StartServer();

    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    };

    // write data
    size_t writesCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
            TString expectedData = makePatten(i);
            memcpy(
                writeBuffer.data(),
                expectedData.data(),
                expectedData.size());
            auto writeOp =
                Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
            EXPECT_EQ(status.size(), writeOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
            ++writesCount;
        }
    }

    // read data
    size_t readsCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
            auto readOp =
                Client.WriteAsync(QueueIndex, {hdr}, {readBuffer, status});
            EXPECT_EQ(status.size() + readBuffer.size(), readOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);

            TString readData(readBuffer.data(), readBuffer.size());
            EXPECT_EQ(makePatten(i), readData);
            ++readsCount;
        }
    }

    // validate storage
    for (ui64 i = 0; i < TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, GetFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writesCount + readsCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readsCount, read.Count);
        EXPECT_EQ(readsCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readsCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writesCount, write.Count);
        EXPECT_EQ(writesCount * RequestSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(
            Unaligned ? writesCount - splittedWrites : 0,
            write.Unaligned);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TServerTest,
    testing::Combine(
        testing::Values(
            NProto::EEncryptionMode::NO_ENCRYPTION,
            NProto::EEncryptionMode::ENCRYPTION_AES_XTS),
        testing::Values(1, 2, 4),       // Blocks per request
        testing::Values(512_B, 4_KB),   // Block size
        testing::Values(true, false),   // Unaligned
        testing::Values(0, 2)),
    [](const testing::TestParamInfo<TTestParams>& info)
    {
        const auto name = TStringBuilder()
                          << std::get<0>(info.param) << "_bc"
                          << std::get<1>(info.param) << "_bs"
                          << std::get<2>(info.param) << "_"
                          << (std::get<3>(info.param) ? "unaligned" : "aligned")
                          << "_tc" << std::get<4>(info.param);
        return std::string(name);
    });

class TSlowEncryptor: public IEncryptor
{
private:
    std::atomic<ui64> SleepTimeMillis;
    std::atomic<ui64> TotalSleepTimeMillis;

    TAdaptiveLock ThreadIdsLock;
    THashSet<ui64> ThreadIds;

    IEncryptorPtr Encryptor;

public:
    explicit TSlowEncryptor(TDuration sleepTime, IEncryptorPtr encryptor)
        : SleepTimeMillis(sleepTime.MilliSeconds())
        , Encryptor(std::move(encryptor))
    {}

    NProto::TError
    Encrypt(TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        const auto sleepTime = SleepTimeMillis.load();
        Sleep(TDuration::MilliSeconds(sleepTime));
        TotalSleepTimeMillis += sleepTime;

        with_lock(ThreadIdsLock) {
            ThreadIds.insert(TThread::CurrentThreadId());
        }

        return Encryptor->Encrypt(src, dst, blockIndex);
    }

    NProto::TError
    Decrypt(TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        const auto sleepTime = SleepTimeMillis.load();
        Sleep(TDuration::MilliSeconds(sleepTime));
        TotalSleepTimeMillis += sleepTime;

        with_lock (ThreadIdsLock) {
            ThreadIds.insert(TThread::CurrentThreadId());
        }

        return Encryptor->Decrypt(src, dst, blockIndex);
    }

    TDuration GetTotalSleepTime() const
    {
        return TDuration::MilliSeconds(TotalSleepTimeMillis.load());
    }

    void SetSleepTime(TDuration sleepTime)
    {
        SleepTimeMillis.store(sleepTime.MilliSeconds());
    }

    THashSet<ui64> GetThreadIds() const
    {
        with_lock(ThreadIdsLock) {
            return ThreadIds;
        }
    }

    void ClearThreadIds()
    {
        with_lock (ThreadIdsLock) {
            ThreadIds.clear();
        }
    }
};

class TSlowEncryptorServerTest: public TServerTest
{
public:
    TSlowEncryptorServerTest()
    {
        Encryptor = std::make_shared<TSlowEncryptor>(
            TDuration::MilliSeconds(100),
            Encryptor);
    }

    TDuration GetTotalSleepTime() const
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        return slowEncryptor->GetTotalSleepTime();
    }

    void SetSleepTime(TDuration sleepTime)
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        slowEncryptor->SetSleepTime(sleepTime);
    }

    THashSet<ui64> GetThreadIds() const
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        return slowEncryptor->GetThreadIds();
    }

    void ClearThreadIds()
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        slowEncryptor->ClearThreadIds();
    }
};

TEST_P(TSlowEncryptorServerTest, ShouldDecryptDataInParallel)
{
    StartServer();

    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    };

    SetSleepTime(TDuration::Zero());

    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
            TString expectedData = makePatten(i);
            memcpy(
                writeBuffer.data(),
                expectedData.data(),
                expectedData.size());
            auto writeOp =
                Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
            EXPECT_EQ(status.size(), writeOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
        }
    }

    SetSleepTime(TDuration::MilliSeconds(100));
    ClearThreadIds();

    // read data
    {
        TVector<std::span<char>> hdrs;
        TVector<std::span<char>> readBuffers;
        TVector<std::span<char>> statuses;
        TVector<NThreading::TFuture<ui32>> readOpsResult;

        for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
            hdrs.push_back(Hdr(Memory, {.type = VIRTIO_BLK_T_IN}));
            readBuffers.push_back(
                Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize));
            statuses.push_back(Memory.Allocate(1));

            reinterpret_cast<virtio_blk_req_hdr*>(hdrs.back().data())->sector =
                i * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdrs.back()},
                {readBuffers.back(), statuses.back()});
            readOpsResult.push_back(readOp);
        }

        auto waitResult = NThreading::WaitAll(readOpsResult);
        waitResult.Wait();

        for (size_t i = 0; i < readOpsResult.size(); ++i) {
            auto readOpResult = readOpsResult[i].GetValueSync();
            EXPECT_EQ(statuses[i].size() + readBuffers[i].size(), readOpResult);
            EXPECT_EQ(VIRTIO_BLK_S_OK, statuses[i][0]);
            TString readData(readBuffers[i].data(), readBuffers[i].size());
            EXPECT_EQ(makePatten(i), readData);
        }

        auto threadIdsCalledDecrypt = GetThreadIds();
        EXPECT_EQ(threadIdsCalledDecrypt.size(), ThreadCount);
    }

    SetSleepTime(TDuration::Zero());

    // validate storage
    for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
        const TString expectedData(BlockSize, GetFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }
}

TEST_P(TSlowEncryptorServerTest, ShouldWaitForAllThreadPoolTasksBeforeStop)
{
    StartServer();

    const ui64 requestCount = ThreadCount * 5;

    auto makePattern = [&](size_t block) -> TString
    {
        return TString(BlockSize, GetFillChar(block));
    };

    // Write blocks directly to the underlying devices. Encrypt them first so
    // that when the server reads + decrypts, we get the original plaintext.
    {
        SetSleepTime(TDuration::Zero());

        TString encrypted;
        encrypted.resize(BlockSize);
        for (ui64 i = 0; i < requestCount; ++i) {
            const TString plaintext = makePattern(i);
            const auto err = Encryptor->Encrypt(
                TBlockDataRef(plaintext.data(), BlockSize),
                TBlockDataRef(encrypted.data(), BlockSize),
                i * SectorsPerBlock);
            ASSERT_FALSE(HasError(err));
            ASSERT_TRUE(SaveRawBlock(i, encrypted));
        }
    }

    SetSleepTime(TDuration::MilliSeconds(100));

    TVector<std::span<char>> readBuffers;
    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;
    for (ui64 i = 0; i < requestCount; ++i) {
        std::span hdr = Hdr(
            Memory,
            {.type = VIRTIO_BLK_T_IN, .sector = i * SectorsPerBlock});
        std::span readBuffer =
            Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        readBuffers.push_back(readBuffer);
        statuses.push_back(status);
        futures.push_back(
            Client.WriteAsync(i % QueueCount, {hdr}, {readBuffer, status}));
    }

    // Give AIO time to complete the reads and enqueue decryption tasks onto
    // the thread pool. Each decryption sleeps 100ms, so most tasks remain
    // queued at this point.
    Sleep(TDuration::MilliSeconds(200));

    Server->Stop();
    Server.reset();

    EXPECT_EQ(
        TDuration::MilliSeconds(requestCount * 100),
        GetTotalSleepTime());

    ASSERT_TRUE(
        NThreading::WaitAll(futures).Wait(TDuration::Seconds(5)))
        << "client did not observe all completed requests";

    for (size_t i = 0; i < futures.size(); ++i) {
        ASSERT_TRUE(futures[i].HasValue())
            << "future " << i << " did not resolve";
        const ui32 len = futures[i].GetValueSync();
        EXPECT_EQ(statuses[i].size() + readBuffers[i].size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, statuses[i][0]);
        TString readData(readBuffers[i].data(), readBuffers[i].size());
        EXPECT_EQ(makePattern(i), readData);
    }

    Client.DeInit();
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TSlowEncryptorServerTest,
    testing::Combine(
        testing::Values(NProto::EEncryptionMode::ENCRYPTION_AES_XTS),
        testing::Values(1, 2, 4),       // Blocks per request
        testing::Values(512_B),         // Block size
        testing::Values(true, false),   // Unaligned
        testing::Values(2, 4)           // Thread Count
        ),
    [](const testing::TestParamInfo<TTestParams>& info)
    {
        const auto name = TStringBuilder()
                          << std::get<0>(info.param) << "_bc"
                          << std::get<1>(info.param) << "_bs"
                          << std::get<2>(info.param) << "_"
                          << (std::get<3>(info.param) ? "unaligned" : "aligned")
                          << "_tc" << std::get<4>(info.param);
        return std::string(name);
    });

}   // namespace NCloud::NBlockStore::NVHostServer
