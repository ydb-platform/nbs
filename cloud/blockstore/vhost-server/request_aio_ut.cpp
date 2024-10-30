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
};

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
