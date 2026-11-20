#include "blob_buffer.h"

#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

class TCountingAllocator final: public IAllocator
{
public:
    size_t Allocations = 0;
    size_t Releases = 0;
    size_t Bytes = 0;

    TBlock Allocate(size_t size) override
    {
        auto block = TDefaultAllocator::Instance()->Allocate(size);
        ++Allocations;
        Bytes += block.Len;
        return block;
    }

    void Release(const TBlock& block) override
    {
        UNIT_ASSERT(Bytes >= block.Len);
        ++Releases;
        Bytes -= block.Len;
        TDefaultAllocator::Instance()->Release(block);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TBlobBufferTest)
{
    Y_UNIT_TEST(ShouldUseOneStableZeroedAllocation)
    {
        TCountingAllocator allocator;
        {
            auto buffer = TBlobBuffer::Allocate(4096, 3, &allocator);
            buffer.AddBlock(4096, 'a');
            const char* first = buffer.GetBlock(0).Data();
            buffer.AddBlock(4096, 0);
            buffer.AddBlock(4096, 'c');
            UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, 1);
            UNIT_ASSERT_VALUES_EQUAL(buffer.GetBytesCount(), 12288);
            UNIT_ASSERT(buffer.GetBlock(0).Data() == first);
            UNIT_ASSERT(buffer.GetBlock(1).Data() == first + 4096);
            UNIT_ASSERT(buffer.GetBlock(2).Data() == first + 8192);
            UNIT_ASSERT_VALUES_EQUAL(
                TString(buffer.GetBlock(1).Data(), 4096), TString(4096, 0));
            auto content = buffer.ShareContent();
            UNIT_ASSERT(content.GetData() == first);
            UNIT_ASSERT_VALUES_EQUAL(content.GetSize(), 12288);
            UNIT_ASSERT_VALUES_EQUAL(content.GetOccupiedMemorySize(), allocator.Bytes);
        }
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 1);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, 0);
    }

    Y_UNIT_TEST(ShouldKeepOwnerAndCancellationGateSeparate)
    {
        TCountingAllocator allocator;
        ::TRcBuf acceptedPayload;
        TGuardedSgList access;
        {
            auto buffer = TBlobBuffer::Allocate(512, 2, &allocator);
            buffer.AddBlock(512, 'a');
            buffer.AddBlock(512, 'b');
            TGuardedBuffer<TBlobBuffer> producer(std::move(buffer));
            access = producer.GetGuardedSgList();
            {
                auto guard = access.Acquire();
                UNIT_ASSERT(guard);
                acceptedPayload = producer.Get().ShareContent();
                UNIT_ASSERT(acceptedPayload.GetData() == guard.Get()[0].Data());
            }
        }
        // Pending requests are cancelled, but an accepted owned payload does
        // not dangle when the producer or another consumer fails.
        UNIT_ASSERT(!access.Acquire());
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(acceptedPayload.GetData(), acceptedPayload.GetSize()),
            TString(512, 'a') + TString(512, 'b'));
        acceptedPayload = {};
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 1);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, 0);
    }

    Y_UNIT_TEST(ShouldAccountForSharedSlicesAndCopyOnWrite)
    {
        TCountingAllocator allocator;
        {
            auto buffer = TBlobBuffer::Allocate(512, 2, &allocator);
            buffer.AddBlock(512, 'a');
            buffer.AddBlock(512, 'b');
            auto content = buffer.ShareContent();
            ::TRcBuf slice(
                ::TRcBuf::Piece, content.GetData() + 512, 16, content);
            UNIT_ASSERT_VALUES_EQUAL(slice.GetOccupiedMemorySize(), allocator.Bytes);
            auto copy = content;
            copy.GetDataMut()[0] = 'c';
            UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, 2);
            UNIT_ASSERT_VALUES_EQUAL(content.GetData()[0], 'a');
            UNIT_ASSERT_VALUES_EQUAL(copy.GetData()[0], 'c');
            UNIT_ASSERT_VALUES_EQUAL(slice.GetData()[0], 'b');
        }
        UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, allocator.Releases);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, 0);
    }

    Y_UNIT_TEST(ShouldKeepOtherOwnedConsumersAliveAfterOneFails)
    {
        TCountingAllocator allocator;
        const TString expected =
            TString(512, 'a') + TString(512, '\0') + TString(512, 'c');
        ::TRcBuf failedConsumer;
        ::TRcBuf firstSurvivor;
        ::TRcBuf secondSurvivor;
        TGuardedSgList failedAccess;
        TGuardedSgList survivingAccess;
        size_t payloadBytes = 0;
        {
            auto buffer = TBlobBuffer::Allocate(512, 3, &allocator);
            buffer.AddBlock(512, 'a');
            buffer.AddBlock(512, 0);
            buffer.AddBlock(512, 'c');
            TGuardedBuffer<TBlobBuffer> producer(std::move(buffer));
            const auto access = producer.GetGuardedSgList();
            failedAccess = access.CreateDepender();
            survivingAccess = access.CreateDepender();
            {
                auto guard = failedAccess.Acquire();
                UNIT_ASSERT(guard);
                failedConsumer = producer.Get().ShareContent();
            }
            {
                auto guard = survivingAccess.Acquire();
                UNIT_ASSERT(guard);
                firstSurvivor = producer.Get().ShareContent();
                secondSurvivor = producer.Get().ShareContent();
                UNIT_ASSERT(
                    failedConsumer.GetData() == firstSurvivor.GetData());
                UNIT_ASSERT(
                    firstSurvivor.GetData() == secondSurvivor.GetData());
            }

            // A consumer failure closes only its dependent gate. No guard is
            // held in this thread while Close runs, and another consumer can
            // still accept the producer's stable bytes.
            failedAccess.Close();
            UNIT_ASSERT(!failedAccess.Acquire());
            UNIT_ASSERT(survivingAccess.Acquire());
            UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, 1);
            UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 0);
            payloadBytes = allocator.Bytes;
            UNIT_ASSERT(payloadBytes >= expected.size());
        }

        // Producer teardown closes the parent gate. Previously accepted
        // immutable payloads are real owners and continue independently.
        UNIT_ASSERT(!survivingAccess.Acquire());
        failedConsumer = {};
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 0);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, payloadBytes);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(firstSurvivor.GetData(), firstSurvivor.GetSize()), expected);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(secondSurvivor.GetData(), secondSurvivor.GetSize()), expected);

        firstSurvivor = {};
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(secondSurvivor.GetData(), secondSurvivor.GetSize()), expected);
        secondSurvivor = {};
        UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, 1);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Releases, 1);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, 0);
    }

    Y_UNIT_TEST(ShouldRetainLegacyRepresentationForPatching)
    {
        TCountingAllocator allocator;
        {
            TBlobBuffer buffer(&allocator);
            buffer.AddBlock(512, 'a');
            buffer.AddBlock(512, 0);
            UNIT_ASSERT(!buffer.IsContiguous());
            UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, 2);
            UNIT_ASSERT_VALUES_EQUAL(
                buffer.AsString(), TString(512, 'a') + TString(512, 0));
        }
        UNIT_ASSERT_VALUES_EQUAL(allocator.Allocations, allocator.Releases);
        UNIT_ASSERT_VALUES_EQUAL(allocator.Bytes, 0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
