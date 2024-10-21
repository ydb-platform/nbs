#include "request_aio.h"

#include <cloud/contrib/vhost/bio.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <array>
#include <random>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

/* libvhost virtio-blk private IO structure */
struct virtio_blk_io {
    void* opaque[2];
    struct vhd_io io;
    struct vhd_bdev_io bdev_io;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBackend
    : public NUnitTest::TBaseFixture
{
    constexpr static int SplitFileHandle = 42;

    TVector<TAioDevice> Devices;
    TLog Log;

    ~TTestBackend()
    {
        ClearDevices();
    }

    void InitSplitDevices(size_t count, i64 fileLen, ui32 blockSize)
    {
        TVector<ui64> offsets(count);
        std::generate_n(offsets.begin(), count, [&, offset = 0] () mutable {
            return std::exchange(offset, offset + fileLen);
        });

        std::shuffle(offsets.begin(), offsets.end(), std::mt19937 {});

        ClearDevices();
        Devices.reserve(count);

        ui64 totalBytes = 0;

        for (size_t i = 0; i != count; ++i) {
            Devices.push_back({
                .StartOffset = totalBytes,
                .EndOffset = totalBytes + fileLen,
                .File = TFileHandle { SplitFileHandle },
                .FileOffset = offsets[i],
                .BlockSize = blockSize
            });
            totalBytes += fileLen;
        }
    }

    void InitDevices(i64 fileLen, ui32 blockSize)
    {
        ClearDevices();
        Devices.reserve(5);

        ui64 totalBytes = 0;

        for (int i = 0; i != 5; ++i) {
            Devices.push_back({
                .StartOffset = totalBytes,
                .EndOffset = totalBytes + fileLen,
                .File = TFileHandle { 100 + i },
                .BlockSize = blockSize
            });
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

Y_UNIT_TEST_SUITE(TAioRequestTest)
{
    Y_UNIT_TEST_F(ShouldPrepareIO, TTestBackend)
    {
        InitDevices(93_GB, 512_B);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 4_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000000),
                .len  = 12_KB
            }
        };

        const i64 offset = 2 * 93_GB + 1_MB; // device #2
        const i64 size = 16_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = offset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(1, batch.size());
        auto req = TAioRequest::FromIocb(batch[0]);

        UNIT_ASSERT_VALUES_EQUAL(nullptr, req->data);
        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_EQUAL(IO_CMD_PREADV, req->aio_lio_opcode);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[2].File), req->aio_fildes);
        UNIT_ASSERT_VALUES_EQUAL(buffers.size(), req->u.c.nbytes);
        UNIT_ASSERT_VALUES_EQUAL(offset - Devices[2].StartOffset, req->u.c.offset);

        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT(!req->Unaligned);
        UNIT_ASSERT(!req->BufferAllocated);

        UNIT_ASSERT_VALUES_EQUAL(buffers[0].base, req->Data[0].iov_base);
        UNIT_ASSERT_VALUES_EQUAL(buffers[0].len, req->Data[0].iov_len);

        UNIT_ASSERT_VALUES_EQUAL(buffers[1].base, req->Data[1].iov_base);
        UNIT_ASSERT_VALUES_EQUAL(buffers[1].len, req->Data[1].iov_len);
    }

    Y_UNIT_TEST_F(ShouldAllocateBounceBuf, TTestBackend)
    {
        InitDevices(93_GB, 512_B);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 4_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000008), // unaligned buffer
                .len  = 12_KB
            }
        };

        const i64 offset = 2 * 93_GB + 1_MB; // device #102
        const i64 size = 16_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = offset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(1, batch.size());
        auto req = TAioRequest::FromIocb(batch[0]);

        UNIT_ASSERT_VALUES_EQUAL(nullptr, req->data);
        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_EQUAL(IO_CMD_PREADV, req->aio_lio_opcode);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[2].File), req->aio_fildes);
        UNIT_ASSERT_VALUES_EQUAL(1, req->u.c.nbytes);
        UNIT_ASSERT_VALUES_EQUAL(offset - Devices[2].StartOffset, req->u.c.offset);

        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT(req->Unaligned);
        UNIT_ASSERT(req->BufferAllocated);

        UNIT_ASSERT_VALUES_UNEQUAL(buffers[0].base, req->Data[0].iov_base);
        UNIT_ASSERT_VALUES_UNEQUAL(buffers[1].base, req->Data[0].iov_base);

        UNIT_ASSERT_VALUES_EQUAL(size, req->Data[0].iov_len);
    }

    Y_UNIT_TEST_F(ShouldPrepareCompoundIO, TTestBackend)
    {
        InitDevices(93_GB, 512_B);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 4_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000000),
                .len  = 12_KB
            }
        };

        const i64 offset = 2 * 93_GB - 6_KB; // devices #1 & #2
        const i64 size = 16_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = offset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(2, batch.size());
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, batch[0]->data);

        auto sub1 = TAioSubRequest::FromIocb(batch[0]);
        auto sub2 = TAioSubRequest::FromIocb(batch[1]);
        auto req = sub1->GetParentRequest();
        UNIT_ASSERT_VALUES_EQUAL(req, sub2->GetParentRequest());

        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[1]->data);
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), req->Inflight.load());
        UNIT_ASSERT_VALUES_EQUAL(0, req->Errors.load());
        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, req->Buffer.get());

        {
            iocb* sub = sub1.get();

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[1].File), sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(6_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(offset - Devices[1].StartOffset, sub->u.c.offset);
        }

        {
            iocb* sub = sub2.get();

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[2].File), sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(10_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(0, sub->u.c.offset);
        }

        auto holder = sub1->TakeParentRequest();
    }

    Y_UNIT_TEST_F(ShouldPrepareCompoundIOForSmallDevices, TTestBackend)
    {
        InitDevices(1_MB, 512_B);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 1_MB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000000),
                .len  = 1_MB + 128_KB
            }
        };

        const i64 offset = 2_MB - 128_KB; // devices [ #1, #2, #3 ]
        const i64 size = 2_MB + 128_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = offset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(3, batch.size());
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, batch[0]->data);

        auto sub1 = TAioSubRequest::FromIocb(batch[0]);
        auto sub2 = TAioSubRequest::FromIocb(batch[1]);
        auto sub3 = TAioSubRequest::FromIocb(batch[2]);
        auto req = sub1->GetParentRequest();

        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[0]->data);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[1]->data);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[2]->data);
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), req->Inflight.load());
        UNIT_ASSERT_VALUES_EQUAL(0, req->Errors.load());
        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, req->Buffer.get());

        {
            iocb* sub = sub1.get();

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[1].File), sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get(), sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(128_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, Devices[1].StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(offset - Devices[1].StartOffset, sub->u.c.offset);
        }

        {
            iocb* sub = sub2.get();

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[2].File), sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, Devices[2].StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get() + 128_KB, sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(0, sub->u.c.offset);
        }

        {
            iocb* sub = sub3.get();

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(Devices[3].File), sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(3_MB, Devices[3].StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get() + 128_KB + 1_MB, sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(0, sub->u.c.offset);
        }

        auto holder = sub1->TakeParentRequest();
    }

    Y_UNIT_TEST_F(ShouldPrepareIOForSplitDevices, TTestBackend)
    {
        InitSplitDevices(5, 1_MB, 512_B);

        // read the 1st device
        {
            std::array buffers {
                vhd_buffer {
                    .base = reinterpret_cast<void*>(0x1000000),
                    .len  = 256_KB
                },
                vhd_buffer {
                    .base = reinterpret_cast<void*>(0x2000000),
                    .len  = 768_KB
                }
            };

            const i64 logicalOffset = 0;
            const i64 size = 1_MB;

            virtio_blk_io bio {
                .bdev_io = {
                    .type = VHD_BDEV_READ,
                    .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                    .total_sectors = size / VHD_SECTOR_SIZE,
                    .sglist = {
                        .nbuffers = buffers.size(),
                        .buffers = buffers.data()
                    }
                }
            };

            const ui64 now = GetCycleCount();

            TVector<iocb*> batch;

            TSimpleStats queueStats;
            PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

            UNIT_ASSERT_VALUES_EQUAL(1, batch.size());
            auto req = TAioRequest::FromIocb(batch[0]);
            batch.clear();

            UNIT_ASSERT_VALUES_EQUAL(nullptr, req->data);
            UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
            UNIT_ASSERT_EQUAL(IO_CMD_PREADV, req->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, req->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(buffers.size(), req->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(Devices[0].FileOffset, req->u.c.offset);

            UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
            UNIT_ASSERT(!req->Unaligned);
            UNIT_ASSERT(!req->BufferAllocated);

            UNIT_ASSERT_VALUES_EQUAL(buffers[0].base, req->Data[0].iov_base);
            UNIT_ASSERT_VALUES_EQUAL(buffers[0].len, req->Data[0].iov_len);

            UNIT_ASSERT_VALUES_EQUAL(buffers[1].base, req->Data[1].iov_base);
            UNIT_ASSERT_VALUES_EQUAL(buffers[1].len, req->Data[1].iov_len);
        }

        // read the 2nd device
        {
            std::array buffers {
                vhd_buffer {
                    .base = reinterpret_cast<void*>(0x1000000),
                    .len  = 192_KB
                },
                vhd_buffer {
                    .base = reinterpret_cast<void*>(0x2000000),
                    .len  = 64_KB
                }
            };

            const i64 logicalOffset = 1_MB + 512_KB;
            const i64 size = 256_KB;

            virtio_blk_io bio {
                .bdev_io = {
                    .type = VHD_BDEV_READ,
                    .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                    .total_sectors = size / VHD_SECTOR_SIZE,
                    .sglist = {
                        .nbuffers = buffers.size(),
                        .buffers = buffers.data()
                    }
                }
            };

            const ui64 now = GetCycleCount();

            TVector<iocb*> batch;
            TSimpleStats queueStats;
            PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

            UNIT_ASSERT_VALUES_EQUAL(1, batch.size());
            auto req = TAioRequest::FromIocb(batch[0]);
            batch.clear();

            UNIT_ASSERT_VALUES_EQUAL(nullptr, req->data);
            UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
            UNIT_ASSERT_EQUAL(IO_CMD_PREADV, req->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, req->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(buffers.size(), req->u.c.nbytes);

            UNIT_ASSERT_VALUES_EQUAL(
                Devices[1].FileOffset + 512_KB,
                req->u.c.offset);

            UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
            UNIT_ASSERT(!req->Unaligned);
            UNIT_ASSERT(!req->BufferAllocated);

            UNIT_ASSERT_VALUES_EQUAL(buffers[0].base, req->Data[0].iov_base);
            UNIT_ASSERT_VALUES_EQUAL(buffers[0].len, req->Data[0].iov_len);

            UNIT_ASSERT_VALUES_EQUAL(buffers[1].base, req->Data[1].iov_base);
            UNIT_ASSERT_VALUES_EQUAL(buffers[1].len, req->Data[1].iov_len);
        }
    }

    Y_UNIT_TEST_F(ShouldPrepareCompoundIOForSplitDevices, TTestBackend)
    {
        InitSplitDevices(5, 1_MB, 512_B);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 256_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000000),
                .len  = 1472_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x3000000),
                .len  = 64_KB
            }
        };

        const i64 logicalOffset = 512_KB;
        const i64 size = 1792_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(3, batch.size());
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, batch[0]->data);

        auto sub1 = TAioSubRequest::FromIocb(batch[0]);
        auto sub2 = TAioSubRequest::FromIocb(batch[1]);
        auto sub3 = TAioSubRequest::FromIocb(batch[2]);
        auto req = sub1->GetParentRequest();

        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[1]->data);
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), req->Inflight.load());
        UNIT_ASSERT_VALUES_EQUAL(0, req->Errors.load());
        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, req->Buffer.get());

        {
            iocb* sub = sub1.get();
            TAioDevice& device = Devices[0];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get(), sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(512_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(0, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                device.FileOffset + logicalOffset - device.StartOffset,
                sub->u.c.offset);
        }

        {
            iocb* sub = sub2.get();
            TAioDevice& device = Devices[1];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get() + 512_KB, sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(device.FileOffset, sub->u.c.offset);
        }

        {
            iocb* sub = sub3.get();
            TAioDevice& device = Devices[2];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                req->Buffer.get() + 512_KB + 1_MB,
                sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(256_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(device.FileOffset, sub->u.c.offset);
        }

        auto holder = sub1->TakeParentRequest();
    }

    Y_UNIT_TEST_F(ShouldPrepareCompoundIOForSplitDevices4KB, TTestBackend)
    {
        InitSplitDevices(5, 1_MB, 4_KB);

        std::array buffers {
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x1000000),
                .len  = 256_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x2000000),
                .len  = 1472_KB
            },
            vhd_buffer {
                .base = reinterpret_cast<void*>(0x3000000),
                .len  = 64_KB
            }
        };

        const i64 logicalOffset = 512_KB;
        const i64 size = 1792_KB;

        virtio_blk_io bio {
            .bdev_io = {
                .type = VHD_BDEV_READ,
                .first_sector = logicalOffset / VHD_SECTOR_SIZE,
                .total_sectors = size / VHD_SECTOR_SIZE,
                .sglist = {
                    .nbuffers = buffers.size(),
                    .buffers = buffers.data()
                }
            }
        };

        const ui64 now = GetCycleCount();

        TVector<iocb*> batch;
        TSimpleStats queueStats;
        PrepareIO(Log, nullptr, Devices, &bio.io, batch, now, queueStats);

        UNIT_ASSERT_VALUES_EQUAL(3, batch.size());
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, batch[0]->data);

        auto sub1 = TAioSubRequest::FromIocb(batch[0]);
        auto sub2 = TAioSubRequest::FromIocb(batch[1]);
        auto sub3 = TAioSubRequest::FromIocb(batch[2]);
        auto req = sub1->GetParentRequest();

        UNIT_ASSERT_VALUES_EQUAL(now, req->SubmitTs);
        UNIT_ASSERT_VALUES_EQUAL(req, batch[1]->data);
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), req->Inflight.load());
        UNIT_ASSERT_VALUES_EQUAL(0, req->Errors.load());
        UNIT_ASSERT_VALUES_EQUAL(&bio.io, req->Io);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, req->Buffer.get());

        {
            iocb* sub = sub1.get();
            TAioDevice& device = Devices[0];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get(), sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(512_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(0, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                device.FileOffset + logicalOffset - device.StartOffset,
                sub->u.c.offset);
        }

        {
            iocb* sub = sub2.get();
            TAioDevice& device = Devices[1];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(req->Buffer.get() + 512_KB, sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(1_MB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(device.FileOffset, sub->u.c.offset);
        }

        {
            iocb* sub = sub3.get();
            TAioDevice& device = Devices[2];

            UNIT_ASSERT_EQUAL(IO_CMD_PREAD, sub->aio_lio_opcode);
            UNIT_ASSERT_VALUES_EQUAL(SplitFileHandle, sub->aio_fildes);
            UNIT_ASSERT_VALUES_EQUAL(2_MB, device.StartOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                req->Buffer.get() + 512_KB + 1_MB,
                sub->u.c.buf);
            UNIT_ASSERT_VALUES_EQUAL(256_KB, sub->u.c.nbytes);
            UNIT_ASSERT_VALUES_EQUAL(device.FileOffset, sub->u.c.offset);
        }

        auto holder = sub1->TakeParentRequest();
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
