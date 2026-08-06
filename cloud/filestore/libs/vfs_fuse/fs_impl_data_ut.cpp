#include "fs_impl_data.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <sys/uio.h>

namespace NCloud::NFileStore::NFuse {

using NProto::TIovec;

namespace {

////////////////////////////////////////////////////////////////////////////////

// fuse_bufvec has a trailing flexible array (`struct fuse_buf buf[1]`), so we
// over-allocate to hold `count` buffers and access them through the struct.
struct TWriteBufVecHolder
{
    TVector<char> Storage;

    explicit TWriteBufVecHolder(size_t count)
        : Storage(sizeof(fuse_bufvec) + (count - 1) * sizeof(fuse_buf), 0)
    {
        Get()->count = count;
    }

    fuse_bufvec* Get()
    {
        return reinterpret_cast<fuse_bufvec*>(Storage.data());
    }

    void SetBuf(size_t index, void* mem, size_t size)
    {
        Get()->buf[index].mem = mem;
        Get()->buf[index].size = size;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFsImplDataTest)
{
    Y_UNIT_TEST(ShouldFillReadIovecs)
    {
        char data0[100];
        char data1[100];
        struct iovec iov[] = {
            {data0, sizeof(data0)},
            {data1, sizeof(data1)},
        };

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        auto error = FillReadDataIovecs(iov, 2, 150, &iovecs);

        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(2, iovecs.size());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data0),
            iovecs[0].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(100, iovecs[0].GetLength());

        // the last segment is capped to the remaining requested length
        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data1),
            iovecs[1].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(50, iovecs[1].GetLength());
    }

    Y_UNIT_TEST(ShouldFillReadIovecsCoveringFullLength)
    {
        char data0[100];
        char data1[100];
        struct iovec iov[] = {
            {data0, sizeof(data0)},
            {data1, sizeof(data1)},
        };

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        auto error = FillReadDataIovecs(iov, 2, 200, &iovecs);

        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(2, iovecs.size());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data0),
            iovecs[0].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(100, iovecs[0].GetLength());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data1),
            iovecs[1].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(100, iovecs[1].GetLength());
    }

    Y_UNIT_TEST(ShouldStopFillingReadIovecsOnceLengthCovered)
    {
        char data0[100];
        char data1[100];
        struct iovec iov[] = {
            {data0, sizeof(data0)},
            {data1, sizeof(data1)},
        };

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        auto error = FillReadDataIovecs(iov, 2, 50, &iovecs);

        UNIT_ASSERT_C(!HasError(error), error.GetMessage());
        // the second data buffer is not needed
        UNIT_ASSERT_VALUES_EQUAL(1, iovecs.size());
        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data0),
            iovecs[0].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(50, iovecs[0].GetLength());
    }

    Y_UNIT_TEST(ShouldFailFillingReadIovecsWhenBuffersTooSmall)
    {
        char data0[100];
        char data1[100];
        struct iovec iov[] = {
            {data0, sizeof(data0)},
            {data1, sizeof(data1)},
        };

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        auto error = FillReadDataIovecs(iov, 2, 250, &iovecs);

        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_FS_INVAL, error.GetCode());
    }

    Y_UNIT_TEST(ShouldFillWriteIovecs)
    {
        char data0[10];
        char data1[20];

        TWriteBufVecHolder bufv(2);
        bufv.SetBuf(0, data0, sizeof(data0));
        bufv.SetBuf(1, data1, sizeof(data1));

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        FillWriteDataIovecs(bufv.Get(), &iovecs);

        UNIT_ASSERT_VALUES_EQUAL(2, iovecs.size());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data0),
            iovecs[0].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(10, iovecs[0].GetLength());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data1),
            iovecs[1].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(20, iovecs[1].GetLength());
    }

    Y_UNIT_TEST(ShouldSkipZeroSizeWriteBuffers)
    {
        char data0[10];
        char data2[30];

        TWriteBufVecHolder bufv(3);
        bufv.SetBuf(0, data0, sizeof(data0));
        bufv.SetBuf(1, nullptr, 0);
        bufv.SetBuf(2, data2, sizeof(data2));

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        FillWriteDataIovecs(bufv.Get(), &iovecs);

        UNIT_ASSERT_VALUES_EQUAL(2, iovecs.size());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data0),
            iovecs[0].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(10, iovecs[0].GetLength());

        UNIT_ASSERT_VALUES_EQUAL(
            reinterpret_cast<ui64>(data2),
            iovecs[1].GetBase());
        UNIT_ASSERT_VALUES_EQUAL(30, iovecs[1].GetLength());
    }

    Y_UNIT_TEST(ShouldProduceNoIovecsForEmptyWrite)
    {
        TWriteBufVecHolder bufv(1);
        bufv.SetBuf(0, nullptr, 0);

        google::protobuf::RepeatedPtrField<TIovec> iovecs;
        FillWriteDataIovecs(bufv.Get(), &iovecs);

        UNIT_ASSERT_VALUES_EQUAL(0, iovecs.size());
    }
}

}   // namespace NCloud::NFileStore::NFuse
