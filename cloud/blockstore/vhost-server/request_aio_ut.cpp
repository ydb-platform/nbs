#include "request_aio.h"

#include <cloud/contrib/vhost/bio.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <random>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

/* libvhost virtio-blk private IO structure */
struct virtio_blk_io
{
    void* opaque[2];
    struct vhd_io io;
    struct vhd_bdev_io bdev_io;
};

////////////////////////////////////////////////////////////////////////////////

struct TCompletedBio
{
    vhd_io* Io = nullptr;
    vhd_bdev_io_result Status = VHD_BDEV_SUCCESS;
};

// Filled by CompleteBio() instead of vhd_complete_bio(), which needs a live
// libvhost request queue.
TVector<TCompletedBio> CompletedBios;

void CompleteBio(vhd_io* io, vhd_bdev_io_result status)
{
    CompletedBios.push_back({.Io = io, .Status = status});
}

class TCountingEncryptor final: public IEncryptor
{
public:
    ui32 DecryptCount = 0;

    NProto::TError Encrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(blockIndex);
        std::memcpy(const_cast<char*>(dst.Data()), src.Data(), src.Size());
        return {};
    }

    NProto::TError Decrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(blockIndex);
        ++DecryptCount;
        std::memcpy(const_cast<char*>(dst.Data()), src.Data(), src.Size());
        return {};
    }
};

template <typename THist>
ui64 GetTotalCount(const THist& hist)
{
    ui64 total = 0;
    hist.IterateBuckets([&](ui64, ui64, ui64 count) { total += count; });
    return total;
}

////////////////////////////////////////////////////////////////////////////////

using TBlockSize = ui32;

class TRequestAIOTest: public testing::TestWithParam<TBlockSize>
{
protected:
    constexpr static int SplitFileHandle = 42;

    TVector<TAioDevice> Devices;
    TLog Log;

public:
    TRequestAIOTest() = default;
    ~TRequestAIOTest() override = default;

    void SetUp() override
    {
        CompletedBios.clear();
    }

    void TearDown() override
    {
        ClearDevices();
    }

    void InitSplitDevices(size_t count, ui64 fileLen)
    {
        const ui32 blockSize = GetParam();
        TVector<ui64> offsets(count);
        std::generate_n(
            offsets.begin(),
            count,
            [&, offset = 0]() mutable
            { return std::exchange(offset, offset + fileLen); });

        std::shuffle(offsets.begin(), offsets.end(), std::mt19937{});

        ClearDevices();
        Devices.reserve(count);

        ui64 totalBytes = 0;

        for (size_t i = 0; i != count; ++i) {
            Devices.push_back(
                {.StartOffset = totalBytes,
                 .EndOffset = totalBytes + fileLen,
                 .File = TFileHandle{SplitFileHandle},
                 .FileOffset = offsets[i],
                 .BlockSize = blockSize});
            totalBytes += fileLen;
        }
    }

    void InitDevices(i64 fileLen)
    {
        const ui32 blockSize = GetParam();
        ClearDevices();
        Devices.reserve(5);

        ui64 totalBytes = 0;

        for (int i = 0; i != 5; ++i) {
            Devices.push_back(
                {.StartOffset = totalBytes,
                 .EndOffset = totalBytes + fileLen,
                 .File = TFileHandle{100 + i},
                 .BlockSize = blockSize});
            totalBytes += fileLen;
        }
    }

    void ClearDevices()
    {
        for (auto& d: Devices) {
            d.File.Release();
        }
        Devices.clear();
    }

    TVector<TAioSubRequestHolder> PrepareCompoundIO(
        virtio_blk_io& bio,
        IEncryptor* encryptor = nullptr)
    {
        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(
            Log,
            encryptor,
            Devices,
            &bio.io,
            batch,
            GetCycleCount(),
            queueStats);

        TVector<TAioSubRequestHolder> subs;
        for (iocb* cb: batch) {
            subs.push_back(TAioSubRequest::FromIocb(cb));
        }
        return subs;
    }

    void CompleteSubRequest(
        TAioSubRequestHolder& sub,
        vhd_bdev_io_result status,
        TAtomicStats& stats,
        IEncryptor* encryptor = nullptr)
    {
        CompleteCompoundRequestImpl(
            Log,
            encryptor,
            std::move(sub),
            status,
            stats,
            CompleteBio);
    }
};

void ExpectNoSuccessStats(const TAtomicStats& stats, vhd_bdev_io_type type)
{
    EXPECT_EQ(0u, GetTotalCount(stats.Times[type]));
    EXPECT_EQ(0u, GetTotalCount(stats.Sizes[type]));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST_P(TRequestAIOTest, ShouldPrepareIO)
{
    InitDevices(93_GB);

    std::array buffers{
        vhd_buffer{.base = reinterpret_cast<void*>(0x1000000), .len = 4_KB},
        vhd_buffer{.base = reinterpret_cast<void*>(0x2000000), .len = 12_KB}};

    const ui64 offset = 2 * 93_GB + 1_MB;   // device #2
    const ui64 size = 16_KB;

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    const ui64 now = GetCycleCount();

    TVector<iocb*> batch;
    TSimpleStats queueStats;
    PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

    EXPECT_EQ(1u, batch.size());
    auto req = TAioRequest::FromIocb(batch[0]);

    EXPECT_EQ(nullptr, req->data);
    EXPECT_EQ(now, req->SubmitTs);
    EXPECT_EQ(IO_CMD_PREADV, req->aio_lio_opcode);
    EXPECT_EQ(static_cast<int>(Devices[2].File), req->aio_fildes);
    EXPECT_EQ(buffers.size(), req->u.c.nbytes);
    EXPECT_EQ(
        offset - Devices[2].StartOffset,
        static_cast<ui64>(req->u.c.offset));

    EXPECT_EQ(&bio.io, req->Io);
    EXPECT_FALSE(req->Unaligned);
    EXPECT_FALSE(req->BufferAllocated);

    EXPECT_EQ(buffers[0].base, req->Data[0].iov_base);
    EXPECT_EQ(buffers[0].len, req->Data[0].iov_len);

    EXPECT_EQ(buffers[1].base, req->Data[1].iov_base);
    EXPECT_EQ(buffers[1].len, req->Data[1].iov_len);
}

TEST_P(TRequestAIOTest, ShouldAllocateBounceBuf)
{
    InitDevices(93_GB);

    std::array buffers{
        vhd_buffer{.base = reinterpret_cast<void*>(0x1000000), .len = 4_KB},
        vhd_buffer{
            .base = reinterpret_cast<void*>(0x2000008),   // unaligned buffer
            .len = 12_KB}};

    const ui64 offset = 2 * 93_GB + 1_MB;   // device #102
    const ui64 size = 16_KB;

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    const ui64 now = GetCycleCount();

    TVector<iocb*> batch;
    TSimpleStats queueStats;
    PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

    EXPECT_EQ(1u, batch.size());
    auto req = TAioRequest::FromIocb(batch[0]);

    EXPECT_EQ(nullptr, req->data);
    EXPECT_EQ(now, req->SubmitTs);
    EXPECT_EQ(IO_CMD_PREADV, req->aio_lio_opcode);
    EXPECT_EQ(static_cast<int>(Devices[2].File), req->aio_fildes);
    EXPECT_EQ(1ul, req->u.c.nbytes);
    EXPECT_EQ(
        offset - Devices[2].StartOffset,
        static_cast<ui64>(req->u.c.offset));

    EXPECT_EQ(&bio.io, req->Io);
    EXPECT_TRUE(req->Unaligned);
    EXPECT_TRUE(req->BufferAllocated);

    EXPECT_NE(buffers[0].base, req->Data[0].iov_base);
    EXPECT_NE(buffers[1].base, req->Data[0].iov_base);

    EXPECT_EQ(size, req->Data[0].iov_len);
}

TEST_P(TRequestAIOTest, ShouldPrepareCompoundIO)
{
    InitDevices(93_GB);

    std::array buffers{
        vhd_buffer{.base = reinterpret_cast<void*>(0x1000000), .len = 4_KB},
        vhd_buffer{.base = reinterpret_cast<void*>(0x2000000), .len = 12_KB}};

    const ui64 offset = 2 * 93_GB - 6_KB;   // devices #1 & #2
    const ui64 size = 16_KB;

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    const ui64 now = GetCycleCount();

    TVector<iocb*> batch;
    TSimpleStats queueStats;
    PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

    EXPECT_EQ(2u, batch.size());
    EXPECT_NE(nullptr, batch[0]->data);

    auto sub1 = TAioSubRequest::FromIocb(batch[0]);
    auto sub2 = TAioSubRequest::FromIocb(batch[1]);
    auto req = sub1->GetParentRequest();
    EXPECT_EQ(req, sub2->GetParentRequest());

    EXPECT_EQ(now, req->SubmitTs);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(req),
        reinterpret_cast<uintptr_t>(batch[1]->data));
    EXPECT_EQ(batch.size(), req->Inflight.load());
    EXPECT_EQ(0u, req->Errors.load());
    EXPECT_EQ(&bio.io, req->Io);
    EXPECT_NE(nullptr, req->Buffer.get());

    {
        iocb* sub = sub1.get();

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(static_cast<int>(Devices[1].File), sub->aio_fildes);
        EXPECT_EQ(6_KB, sub->u.c.nbytes);
        EXPECT_EQ(
            offset - Devices[1].StartOffset,
            static_cast<ui64>(sub->u.c.offset));
    }

    {
        iocb* sub = sub2.get();

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(static_cast<int>(Devices[2].File), sub->aio_fildes);
        EXPECT_EQ(10_KB, sub->u.c.nbytes);
        EXPECT_EQ(0, sub->u.c.offset);
    }

    auto holder = sub1->TakeParentRequest();
}

TEST_P(TRequestAIOTest, ShouldPrepareCompoundIOForSmallDevices)
{
    InitDevices(1_MB);

    std::array buffers{
        vhd_buffer{.base = reinterpret_cast<void*>(0x1000000), .len = 1_MB},
        vhd_buffer{
            .base = reinterpret_cast<void*>(0x2000000),
            .len = 1_MB + 128_KB}};

    const ui64 offset = 2_MB - 128_KB;   // devices [ #1, #2, #3 ]
    const ui64 size = 2_MB + 128_KB;

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    const ui64 now = GetCycleCount();

    TVector<iocb*> batch;
    TSimpleStats queueStats;
    PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

    EXPECT_EQ(3u, batch.size());
    EXPECT_NE(nullptr, batch[0]->data);

    auto sub1 = TAioSubRequest::FromIocb(batch[0]);
    auto sub2 = TAioSubRequest::FromIocb(batch[1]);
    auto sub3 = TAioSubRequest::FromIocb(batch[2]);
    auto req = sub1->GetParentRequest();

    EXPECT_EQ(now, req->SubmitTs);
    EXPECT_EQ(req, batch[0]->data);
    EXPECT_EQ(req, batch[1]->data);
    EXPECT_EQ(req, batch[2]->data);
    EXPECT_EQ(batch.size(), req->Inflight.load());
    EXPECT_EQ(0u, req->Errors.load());
    EXPECT_EQ(&bio.io, req->Io);
    EXPECT_NE(nullptr, req->Buffer.get());

    {
        iocb* sub = sub1.get();

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(static_cast<int>(Devices[1].File), sub->aio_fildes);
        EXPECT_EQ(req->Buffer.get(), sub->u.c.buf);
        EXPECT_EQ(128_KB, sub->u.c.nbytes);
        EXPECT_EQ(1_MB, Devices[1].StartOffset);
        EXPECT_EQ(
            offset - Devices[1].StartOffset,
            static_cast<ui64>(sub->u.c.offset));
    }

    {
        iocb* sub = sub2.get();

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(static_cast<int>(Devices[2].File), sub->aio_fildes);
        EXPECT_EQ(2_MB, Devices[2].StartOffset);
        EXPECT_EQ(req->Buffer.get() + 128_KB, sub->u.c.buf);
        EXPECT_EQ(1_MB, sub->u.c.nbytes);
        EXPECT_EQ(0, sub->u.c.offset);
    }

    {
        iocb* sub = sub3.get();

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(static_cast<int>(Devices[3].File), sub->aio_fildes);
        EXPECT_EQ(3_MB, Devices[3].StartOffset);
        EXPECT_EQ(req->Buffer.get() + 128_KB + 1_MB, sub->u.c.buf);
        EXPECT_EQ(1_MB, sub->u.c.nbytes);
        EXPECT_EQ(0, sub->u.c.offset);
    }

    auto holder = sub1->TakeParentRequest();
}

TEST_P(TRequestAIOTest, ShouldPrepareIOForSplitDevices)
{
    InitSplitDevices(5, 1_MB);

    // read the 1st device
    {
        std::array buffers{
            vhd_buffer{
                .base = reinterpret_cast<void*>(0x1000000),
                .len = 256_KB},
            vhd_buffer{
                .base = reinterpret_cast<void*>(0x2000000),
                .len = 768_KB}};

        const ui64 logicalOffset = 0;
        const ui64 size = 1_MB;

        virtio_blk_io bio{
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()}}};

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;

        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        EXPECT_EQ(1u, batch.size());
        auto req = TAioRequest::FromIocb(batch[0]);
        batch.clear();

        EXPECT_EQ(nullptr, req->data);
        EXPECT_EQ(now, req->SubmitTs);
        EXPECT_EQ(IO_CMD_PREADV, req->aio_lio_opcode);
        EXPECT_EQ(SplitFileHandle, req->aio_fildes);
        EXPECT_EQ(buffers.size(), req->u.c.nbytes);
        EXPECT_EQ(Devices[0].FileOffset, static_cast<ui64>(req->u.c.offset));

        EXPECT_EQ(&bio.io, req->Io);
        EXPECT_FALSE(req->Unaligned);
        EXPECT_FALSE(req->BufferAllocated);

        EXPECT_EQ(buffers[0].base, req->Data[0].iov_base);
        EXPECT_EQ(buffers[0].len, req->Data[0].iov_len);

        EXPECT_EQ(buffers[1].base, req->Data[1].iov_base);
        EXPECT_EQ(buffers[1].len, req->Data[1].iov_len);
    }

    // read the 2nd device
    {
        std::array buffers{
            vhd_buffer{
                .base = reinterpret_cast<void*>(0x1000000),
                .len = 192_KB},
            vhd_buffer{
                .base = reinterpret_cast<void*>(0x2000000),
                .len = 64_KB}};

        const ui64 logicalOffset = 1_MB + 512_KB;
        const ui64 size = 256_KB;

        virtio_blk_io bio{
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()}}};

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        EXPECT_EQ(1u, batch.size());
        auto req = TAioRequest::FromIocb(batch[0]);
        batch.clear();

        EXPECT_EQ(nullptr, req->data);
        EXPECT_EQ(now, req->SubmitTs);
        EXPECT_EQ(IO_CMD_PREADV, req->aio_lio_opcode);
        EXPECT_EQ(SplitFileHandle, req->aio_fildes);
        EXPECT_EQ(buffers.size(), req->u.c.nbytes);

        EXPECT_EQ(
            Devices[1].FileOffset + 512_KB,
            static_cast<ui64>(req->u.c.offset));

        EXPECT_EQ(&bio.io, req->Io);
        EXPECT_FALSE(req->Unaligned);
        EXPECT_FALSE(req->BufferAllocated);

        EXPECT_EQ(buffers[0].base, req->Data[0].iov_base);
        EXPECT_EQ(buffers[0].len, req->Data[0].iov_len);

        EXPECT_EQ(buffers[1].base, req->Data[1].iov_base);
        EXPECT_EQ(buffers[1].len, req->Data[1].iov_len);
    }
}

TEST_P(TRequestAIOTest, ShouldPrepareCompoundIOForSplitDevices)
{
    InitSplitDevices(5, 1_MB);

    std::array buffers{
        vhd_buffer{.base = reinterpret_cast<void*>(0x1000000), .len = 256_KB},
        vhd_buffer{.base = reinterpret_cast<void*>(0x2000000), .len = 1472_KB},
        vhd_buffer{.base = reinterpret_cast<void*>(0x3000000), .len = 64_KB}};

    const ui64 logicalOffset = 512_KB;
    const ui64 size = 1792_KB;

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = logicalOffset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    const ui64 now = GetCycleCount();

    TVector<iocb*> batch;
    TSimpleStats queueStats;
    PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

    EXPECT_EQ(3u, batch.size());
    EXPECT_NE(nullptr, batch[0]->data);

    auto sub1 = TAioSubRequest::FromIocb(batch[0]);
    auto sub2 = TAioSubRequest::FromIocb(batch[1]);
    auto sub3 = TAioSubRequest::FromIocb(batch[2]);
    auto req = sub1->GetParentRequest();

    EXPECT_EQ(now, req->SubmitTs);
    EXPECT_EQ(req, batch[1]->data);
    EXPECT_EQ(batch.size(), req->Inflight.load());
    EXPECT_EQ(0u, req->Errors.load());
    EXPECT_EQ(&bio.io, req->Io);
    EXPECT_NE(nullptr, req->Buffer.get());

    {
        iocb* sub = sub1.get();
        TAioDevice& device = Devices[0];

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(SplitFileHandle, sub->aio_fildes);
        EXPECT_EQ(req->Buffer.get(), sub->u.c.buf);
        EXPECT_EQ(512_KB, sub->u.c.nbytes);
        EXPECT_EQ(0u, device.StartOffset);
        EXPECT_EQ(
            device.FileOffset + logicalOffset - device.StartOffset,
            static_cast<ui64>(sub->u.c.offset));
    }

    {
        iocb* sub = sub2.get();
        TAioDevice& device = Devices[1];

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(SplitFileHandle, sub->aio_fildes);
        EXPECT_EQ(1_MB, device.StartOffset);
        EXPECT_EQ(req->Buffer.get() + 512_KB, sub->u.c.buf);
        EXPECT_EQ(1_MB, sub->u.c.nbytes);
        EXPECT_EQ(device.FileOffset, static_cast<ui64>(sub->u.c.offset));
    }

    {
        iocb* sub = sub3.get();
        TAioDevice& device = Devices[2];

        EXPECT_EQ(IO_CMD_PREAD, sub->aio_lio_opcode);
        EXPECT_EQ(SplitFileHandle, sub->aio_fildes);
        EXPECT_EQ(2_MB, device.StartOffset);
        EXPECT_EQ(req->Buffer.get() + 512_KB + 1_MB, sub->u.c.buf);
        EXPECT_EQ(256_KB, sub->u.c.nbytes);
        EXPECT_EQ(device.FileOffset, static_cast<ui64>(sub->u.c.offset));
    }

    auto holder = sub1->TakeParentRequest();
}

TEST_P(TRequestAIOTest, ShouldFailCompoundWriteIfEarlierPartFailed)
{
    InitDevices(1_MB);

    const ui64 offset = 1_MB - 8_KB;   // devices #0 & #1
    const ui64 size = 16_KB;

    TVector<char> guest(size, 'W');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_WRITE,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    auto subs = PrepareCompoundIO(bio);
    ASSERT_EQ(2u, subs.size());

    TAtomicStats stats;

    CompleteSubRequest(subs[0], VHD_BDEV_IOERR, stats);
    EXPECT_TRUE(CompletedBios.empty());

    CompleteSubRequest(subs[1], VHD_BDEV_SUCCESS, stats);
    ASSERT_EQ(1u, CompletedBios.size());
    EXPECT_EQ(&bio.io, CompletedBios[0].Io);
    EXPECT_EQ(VHD_BDEV_IOERR, CompletedBios[0].Status);

    EXPECT_EQ(1u, stats.Requests[VHD_BDEV_WRITE].Errors.load());
    ExpectNoSuccessStats(stats, VHD_BDEV_WRITE);
}

TEST_P(TRequestAIOTest, ShouldNotCopyCompoundReadIfEarlierPartFailed)
{
    InitDevices(1_MB);

    const ui64 offset = 1_MB - 8_KB;   // devices #0 & #1
    const ui64 size = 16_KB;

    TVector<char> guest(size, 'G');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    TCountingEncryptor encryptor;
    auto subs = PrepareCompoundIO(bio, &encryptor);
    ASSERT_EQ(2u, subs.size());

    // Pretend that the subrequests read some data into the shared buffer.
    auto* req = subs[0]->GetParentRequest();
    std::memset(req->Buffer.get(), 'R', req->BufferSize);

    TAtomicStats stats;

    CompleteSubRequest(subs[0], VHD_BDEV_IOERR, stats, &encryptor);
    CompleteSubRequest(subs[1], VHD_BDEV_SUCCESS, stats, &encryptor);

    ASSERT_EQ(1u, CompletedBios.size());
    EXPECT_EQ(VHD_BDEV_IOERR, CompletedBios[0].Status);

    EXPECT_EQ(0u, encryptor.DecryptCount);
    EXPECT_EQ(TVector<char>(size, 'G'), guest);

    EXPECT_EQ(1u, stats.Requests[VHD_BDEV_READ].Errors.load());
    ExpectNoSuccessStats(stats, VHD_BDEV_READ);
}

TEST_P(TRequestAIOTest, ShouldFailCompoundRequestIfLastPartFailed)
{
    InitDevices(1_MB);

    const ui64 offset = 1_MB - 8_KB;   // devices #0 & #1
    const ui64 size = 16_KB;

    TVector<char> guest(size, 'W');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_WRITE,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    auto subs = PrepareCompoundIO(bio);
    ASSERT_EQ(2u, subs.size());

    TAtomicStats stats;

    CompleteSubRequest(subs[0], VHD_BDEV_SUCCESS, stats);
    EXPECT_TRUE(CompletedBios.empty());

    CompleteSubRequest(subs[1], VHD_BDEV_IOERR, stats);
    ASSERT_EQ(1u, CompletedBios.size());
    EXPECT_EQ(VHD_BDEV_IOERR, CompletedBios[0].Status);

    EXPECT_EQ(1u, stats.Requests[VHD_BDEV_WRITE].Errors.load());
    ExpectNoSuccessStats(stats, VHD_BDEV_WRITE);
}

TEST_P(TRequestAIOTest, ShouldFailCompoundRequestIfMiddlePartFailed)
{
    InitDevices(64_KB);

    const ui64 offset = 64_KB - 8_KB;   // devices #0, #1 & #2
    const ui64 size = 80_KB;

    TVector<char> guest(size, 'W');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_WRITE,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    auto subs = PrepareCompoundIO(bio);
    ASSERT_EQ(3u, subs.size());

    TAtomicStats stats;

    CompleteSubRequest(subs[0], VHD_BDEV_SUCCESS, stats);
    CompleteSubRequest(subs[1], VHD_BDEV_IOERR, stats);
    EXPECT_TRUE(CompletedBios.empty());

    CompleteSubRequest(subs[2], VHD_BDEV_SUCCESS, stats);
    ASSERT_EQ(1u, CompletedBios.size());
    EXPECT_EQ(VHD_BDEV_IOERR, CompletedBios[0].Status);

    EXPECT_EQ(1u, stats.Requests[VHD_BDEV_WRITE].Errors.load());
    ExpectNoSuccessStats(stats, VHD_BDEV_WRITE);
}

TEST_P(TRequestAIOTest, ShouldCompleteCompoundReadIfAllPartsSucceeded)
{
    InitDevices(1_MB);

    const ui64 offset = 1_MB - 8_KB;   // devices #0 & #1
    const ui64 size = 16_KB;

    TVector<char> guest(size, 'G');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    virtio_blk_io bio{
        .bdev_io = {
            .type = VHD_BDEV_READ,
            .first_sector = offset / VHD_SECTOR_SIZE,
            .total_sectors = size / VHD_SECTOR_SIZE,
            .sglist = {.nbuffers = buffers.size(), .buffers = buffers.data()}}};

    TCountingEncryptor encryptor;
    auto subs = PrepareCompoundIO(bio, &encryptor);
    ASSERT_EQ(2u, subs.size());

    auto* req = subs[0]->GetParentRequest();
    std::memset(req->Buffer.get(), 'R', req->BufferSize);

    TAtomicStats stats;

    CompleteSubRequest(subs[0], VHD_BDEV_SUCCESS, stats, &encryptor);
    CompleteSubRequest(subs[1], VHD_BDEV_SUCCESS, stats, &encryptor);

    ASSERT_EQ(1u, CompletedBios.size());
    EXPECT_EQ(VHD_BDEV_SUCCESS, CompletedBios[0].Status);

    EXPECT_EQ(size / VHD_SECTOR_SIZE, encryptor.DecryptCount);
    EXPECT_EQ(TVector<char>(size, 'R'), guest);

    EXPECT_EQ(0u, stats.Requests[VHD_BDEV_READ].Errors.load());
    EXPECT_EQ(1u, stats.Requests[VHD_BDEV_READ].Count.load());
    EXPECT_EQ(1u, GetTotalCount(stats.Times[VHD_BDEV_READ]));
    EXPECT_EQ(1u, GetTotalCount(stats.Sizes[VHD_BDEV_READ]));
}

TEST_P(TRequestAIOTest, ShouldCompleteCompoundRequestOnceInAnyOrder)
{
    InitDevices(64_KB);

    const ui64 offset = 64_KB - 8_KB;   // devices #0, #1 & #2
    const ui64 size = 80_KB;

    TVector<char> guest(size, 'W');
    std::array buffers{vhd_buffer{.base = guest.data(), .len = size}};

    // -1 means that all the parts succeed.
    for (int failedPart: {-1, 0, 1, 2}) {
        std::array<size_t, 3> order{0, 1, 2};
        do {
            SCOPED_TRACE(
                TStringBuilder() << "failed part: " << failedPart
                                 << ", order: " << order[0] << order[1]
                                 << order[2]);

            CompletedBios.clear();

            virtio_blk_io bio{
                .bdev_io = {
                    .type = VHD_BDEV_WRITE,
                    .first_sector = offset / VHD_SECTOR_SIZE,
                    .total_sectors = size / VHD_SECTOR_SIZE,
                    .sglist = {
                        .nbuffers = buffers.size(),
                        .buffers = buffers.data()}}};

            auto subs = PrepareCompoundIO(bio);
            ASSERT_EQ(3u, subs.size());

            TAtomicStats stats;
            for (size_t i: order) {
                CompleteSubRequest(
                    subs[i],
                    static_cast<int>(i) == failedPart ? VHD_BDEV_IOERR
                                                      : VHD_BDEV_SUCCESS,
                    stats);
            }

            ASSERT_EQ(1u, CompletedBios.size());
            EXPECT_EQ(&bio.io, CompletedBios[0].Io);
            EXPECT_EQ(
                failedPart == -1 ? VHD_BDEV_SUCCESS : VHD_BDEV_IOERR,
                CompletedBios[0].Status);
        } while (std::next_permutation(order.begin(), order.end()));
    }
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TRequestAIOTest,
    testing::Values(512_B, 4_KB),   // Block size
    [](const testing::TestParamInfo<TBlockSize>& info)
    {
        const auto name = TStringBuilder() << "bs" << info.param;
        return std::string(name);
    });

}   // namespace NCloud::NBlockStore::NVHostServer
