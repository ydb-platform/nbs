#include "key_buffer_store.h"

#include "device.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// 48 bytes of header and 16 bytes of payload per page
constexpr ui32 TestPageSize = 64;
constexpr ui64 TestPageCount = 8;   // 2 superblock slots and 6 entry pages
constexpr ui64 FirstEntryPageNo = 2;

TBuffer MakeBuffer(TStringBuf data)
{
    return TBuffer(data.data(), data.size());
}

TString AsString(const TBuffer& buffer)
{
    return TString(buffer.Data(), buffer.Size());
}

TString Get(const TVector<TKeyBuffer>& buffers, ui64 key)
{
    auto it =
        FindIf(buffers, [key](const auto& entry) { return entry.Key == key; });
    UNIT_ASSERT_C(it != buffers.end(), "key " << key << " is missing");
    return AsString(it->Buffer);
}

TVector<TKeyBuffer> Restore(const IKeyBufferStorePtr& store)
{
    auto response = store->Restore().GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
    return response.ExtractResult();
}

// "<key>=<buffer>|..." in the key order
TString Describe(TVector<TKeyBuffer> buffers)
{
    SortBy(buffers, [](const auto& entry) { return entry.Key; });

    TStringBuilder sb;
    for (const auto& [key, buffer]: buffers) {
        if (sb) {
            sb << "|";
        }
        sb << key << "=" << AsString(buffer);
    }
    return sb;
}

void Write(const IKeyBufferStorePtr& store, ui64 key, TStringBuf data)
{
    const auto error = store->Write(key, MakeBuffer(data)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
}

ui32 EraseBelow(const IKeyBufferStorePtr& store, ui64 key)
{
    const auto error = store->EraseBelow(key).GetValueSync();
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return error.GetCode();
}

////////////////////////////////////////////////////////////////////////////////

ILoggingServicePtr TestLogging()
{
    static const ILoggingServicePtr logging = []
    {
        auto logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        logging->Start();
        return logging;
    }();
    return logging;
}

IKeyBufferStorePtr CreateTestStore(
    const IDevicePtr& device,
    ui64 pageCount = TestPageCount)
{
    return CreateDeviceKeyBufferStore(
        TestLogging(),
        device,
        pageCount,
        TestPageSize);
}

// a store instance restored from the device
IKeyBufferStorePtr OpenTestStore(
    const IDevicePtr& device,
    ui64 pageCount = TestPageCount)
{
    auto store = CreateTestStore(device, pageCount);
    Restore(store);
    return store;
}

// what a fresh store instance restores from the device
TVector<TKeyBuffer> Reopen(
    const IDevicePtr& device,
    ui64 pageCount = TestPageCount)
{
    return Restore(CreateTestStore(device, pageCount));
}

TString ReadFromDevice(const IDevicePtr& device, ui64 pageNo)
{
    NCloud::NProto::TReadPagesRequest request;
    auto& ref = *request.AddPageGroupRefs();
    ref.SetFirstPageNo(pageNo);
    ref.SetPageCount(1);
    ref.SetPageSize(TestPageSize);

    auto response = device->ReadPages(std::move(request)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
    UNIT_ASSERT_VALUES_EQUAL(1, response.GetPageGroups(0).ContentSize());

    return response.GetPageGroups(0).GetContent(0);
}

void WriteToDevice(const IDevicePtr& device, ui64 pageNo, TString content)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    auto& group = *request.AddPageGroups();
    group.SetFirstPageNo(pageNo);
    *group.AddContent() = std::move(content);

    auto response = device->WritePages(std::move(request)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
}

// flips a byte of the page - by default the first payload byte of an entry
// page, which only the checksum guards
void CorruptDevicePage(
    const IDevicePtr& device,
    ui64 pageNo,
    size_t offset = 48)
{
    auto content = ReadFromDevice(device, pageNo);
    content[offset] = static_cast<char>(content[offset] ^ 0xFF);
    WriteToDevice(device, pageNo, std::move(content));
}

bool IsZeroDevicePage(const IDevicePtr& device, ui64 pageNo)
{
    return ReadFromDevice(device, pageNo) == TString(TestPageSize, '\0');
}

////////////////////////////////////////////////////////////////////////////////

// A device that holds its write responses until they are released and fails
// the writes while broken.
class TStuckDevice final: public IDevice
{
private:
    const IDevicePtr Device = CreateInMemoryDevice();

    TVector<std::function<void()>> Pending;

public:
    bool Broken = false;

    NThreading::TFuture<NCloud::NProto::TReadPagesResponse> ReadPages(
        NCloud::NProto::TReadPagesRequest request) override
    {
        return Device->ReadPages(std::move(request));
    }

    NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> WritePages(
        NCloud::NProto::TWriteLogRecordRequest request) override
    {
        if (Broken) {
            return NThreading::MakeFuture<
                NCloud::NProto::TWriteLogRecordResponse>(
                TErrorResponse(E_IO, "device is broken"));
        }

        auto promise =
            NThreading::NewPromise<NCloud::NProto::TWriteLogRecordResponse>();

        Pending.push_back(
            [device = Device, request = std::move(request), promise]() mutable
            {
                promise.SetValue(
                    device->WritePages(std::move(request)).GetValueSync());
            });

        return promise.GetFuture();
    }

    size_t PendingCount() const
    {
        return Pending.size();
    }

    void ReleaseAll()
    {
        auto pending = std::move(Pending);
        Pending.clear();
        for (auto& release: pending) {
            release();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryKeyBufferStoreTest)
{
    Y_UNIT_TEST(ShouldStartEmpty)
    {
        auto store = CreateInMemoryKeyBufferStore();

        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());
    }

    Y_UNIT_TEST(ShouldInsertAndGet)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "one");
        Write(store, 2, "two");

        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("1=one|2=two", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldOverwriteAnExistingKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "first");
        Write(store, 1, "second");

        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("1=second", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldEraseASingleKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "one");

        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());

        // removing what is not there reports that nothing was done
        code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
    }

    Y_UNIT_TEST(ShouldEraseEveryKeyBelowTheGivenOne)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {1, 3, 5, 7}) {
            Write(store, key, "x");
        }

        // the bound itself is kept
        auto code = EraseBelow(store, 5);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("5=x|7=x", Describe(buffers));

        // and it need not be a stored key
        code = EraseBelow(store, 6);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("7=x", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldEraseNothingBelowTheLowestKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 5, "x");

        auto code = EraseBelow(store, 5);
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("5=x", Describe(buffers));

        // and on an empty store
        code = EraseBelow(store, 6);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        code = EraseBelow(store, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
    }

    Y_UNIT_TEST(ShouldEraseKeyZeroLikeAnyOther)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 0, "metadata");
        Write(store, 10, "record");

        // the store gives key 0 no special meaning
        auto code = EraseBelow(store, 0);
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("0=metadata|10=record", Describe(buffers));

        code = EraseBelow(store, 11);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());
    }

    Y_UNIT_TEST(ShouldRefuseToWriteAnErasedKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 5, "x");
        auto code = EraseBelow(store, 6);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        auto error = store->Write(5, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        error = store->Write(3, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        Write(store, 6, "y");
        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("6=y", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldReadKeysInAscendingOrder)
    {
        auto store = CreateInMemoryKeyBufferStore();

        for (ui64 key: {5, 1, 3}) {
            Write(store, key, "x");
        }

        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("1=x|3=x|5=x", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepAnIndependentCopyOfTheBuffer)
    {
        auto store = CreateInMemoryKeyBufferStore();

        TBuffer buffer = MakeBuffer("original");
        auto error = store->Write(1, buffer).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        buffer.Clear();

        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("1=original", Describe(buffers));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceKeyBufferStoreTest)
{
    Y_UNIT_TEST(ShouldStartEmptyOnAFreshDevice)
    {
        auto device = CreateInMemoryDevice();
        auto store = CreateTestStore(device);

        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());
        buffers = Reopen(device);
        UNIT_ASSERT(buffers.empty());
    }

    Y_UNIT_TEST(ShouldRequireRestoreBeforeUse)
    {
        auto store = CreateTestStore(CreateInMemoryDevice());

        auto error = store->Write(1, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
    }

    Y_UNIT_TEST(ShouldKeepTheBuffersAcrossRestores)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, "two");
            Write(store, 3, "");
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=one|2=two|3=", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldSplitABufferAcrossPages)
    {
        auto device = CreateInMemoryDevice();

        // 40 bytes take 3 pages of 16 bytes payload, the last one partially
        const TString data = "0123456789abcdefghijklmnopqrstuvwxyzABCD";
        UNIT_ASSERT_VALUES_EQUAL(40, data.size());

        {
            auto store = OpenTestStore(device);
            Write(store, 1, data);
            Write(store, 2, "0123456789abcdef");   // exactly one page
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(2, buffers.size());
        UNIT_ASSERT_VALUES_EQUAL(data, Get(buffers, 1));
        UNIT_ASSERT_VALUES_EQUAL("0123456789abcdef", Get(buffers, 2));
    }

    Y_UNIT_TEST(ShouldNotTouchThePagesBeyondTheDevice)
    {
        auto device = CreateInMemoryDevice();

        auto store = OpenTestStore(device);
        Write(store, 1, "0123456789abcdefghijklmnopqrstuvwxyzABCD");
        Write(store, 2, "x");
        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        const bool isZero = IsZeroDevicePage(device, TestPageCount);
        UNIT_ASSERT(isZero);
    }

    Y_UNIT_TEST(ShouldRejectABufferThatDoesNotFit)
    {
        auto device = CreateInMemoryDevice();
        auto store = OpenTestStore(device);

        // 6 entry pages of 16 bytes payload each
        auto error =
            store->Write(1, MakeBuffer(TString(97, 'x'))).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        Write(store, 1, TString(96, 'x'));

        error = store->Write(2, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=" + TString(96, 'x'), Describe(buffers));
    }

    Y_UNIT_TEST(ShouldReuseThePagesOfErasedBuffers)
    {
        auto device = CreateInMemoryDevice();
        auto store = OpenTestStore(device);

        for (ui64 key = 1; key <= 6; ++key) {
            Write(store, key, "x");
        }

        auto error = store->Write(7, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto code = EraseBelow(store, 4);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        Write(store, 7, "0123456789abcdefghijklmnopqrstuvwxyzABCD");

        error = store->Write(8, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(
            "4=x|5=x|6=x|7=0123456789abcdefghijklmnopqrstuvwxyzABCD",
            Describe(buffers));
    }

    Y_UNIT_TEST(ShouldPersistTheErasedBound)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, "two");
            Write(store, 3, "three");
            auto code = EraseBelow(store, 3);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        }

        // the erased pages were not reused, the bound drops them anyway
        {
            auto store = CreateTestStore(device);
            auto buffers = Restore(store);
            UNIT_ASSERT_VALUES_EQUAL("3=three", Describe(buffers));

            auto code = EraseBelow(store, 3);
            UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
            auto error = store->Write(2, MakeBuffer("x")).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

            // a bound without live keys is persisted all the same
            code = EraseBelow(store, 4);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
            code = EraseBelow(store, 11);
            UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
        }

        {
            auto store = CreateTestStore(device);
            auto buffers = Restore(store);
            UNIT_ASSERT(buffers.empty());
            auto error = store->Write(10, MakeBuffer("x")).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            Write(store, 11, "x");
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("11=x", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepTheNewestCopyOfARewrittenKey)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "first");
            Write(store, 1, "second");
        }

        // the sequence numbers go on after a restore, so the copy written by
        // a later instance wins even though it may land on lower pages
        {
            auto store = CreateTestStore(device);
            auto buffers = Restore(store);
            UNIT_ASSERT_VALUES_EQUAL("1=second", Describe(buffers));
            Write(store, 1, "third");
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=third", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldFreeThePagesOfTheOverwrittenCopy)
    {
        auto device = CreateInMemoryDevice();
        auto store = OpenTestStore(device);

        for (int i = 0; i < 20; ++i) {
            // 3 pages each time, 6 in the store - the old copy must go
            Write(store, 1, TString(40, static_cast<char>('a' + i)));
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=" + TString(40, 't'), Describe(buffers));
    }

    Y_UNIT_TEST(ShouldDropATornBuffer)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, "0123456789abcdefghijklmnopqrstuvwxyzABCD");
            Write(store, 3, "three");
        }

        // key 2 takes the entry pages 1, 2 and 3
        CorruptDevicePage(device, FirstEntryPageNo + 2);

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=one|3=three", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepTheOldCopyWhenTheRewriteIsTorn)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "first");
            Write(store, 1, "0123456789abcdefghijklmnopqrstuvwxyzABCD");
        }

        // the old copy sits at the entry page 0, the new one at 1, 2 and 3
        CorruptDevicePage(device, FirstEntryPageNo + 1);

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=first", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldSurviveATornSuperblock)
    {
        auto device = CreateInMemoryDevice();

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, "two");
            Write(store, 3, "three");
            auto code = EraseBelow(store, 2);   // slot 0
            UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
            code = EraseBelow(store, 3);   // slot 1
            UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        }

        // the newest superblock is lost, the previous bound applies
        CorruptDevicePage(device, 1, 20);

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("2=two|3=three", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldIgnoreGarbageOnTheDevice)
    {
        auto device = CreateInMemoryDevice();

        for (ui64 pageNo = 0; pageNo < TestPageCount; ++pageNo) {
            WriteToDevice(device, pageNo, TString(TestPageSize, 'g'));
        }

        // the store needs an empty superblock slot to tell that no bound
        // has been persisted
        WriteToDevice(device, 1, TString(TestPageSize, '\0'));

        auto store = CreateTestStore(device);
        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());

        Write(store, 1, "one");
        Write(store, 2, "two");
        buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=one|2=two", Describe(buffers));

        // the superblock goes to the dirty slot, the empty one stays empty
        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        UNIT_ASSERT(ReadFromDevice(device, 0) != TString(TestPageSize, 'g'));
        UNIT_ASSERT(IsZeroDevicePage(device, 1));

        buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("2=two", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldRefuseToRestoreWithBothSuperblockSlotsDirty)
    {
        auto device = CreateInMemoryDevice();
        WriteToDevice(device, 0, TString(TestPageSize, 'g'));
        WriteToDevice(device, 1, TString(TestPageSize, 'g'));

        auto store = CreateTestStore(device);
        auto response = store->Restore().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldRestoreALargeRangeInSeveralReads)
    {
        auto device = CreateInMemoryDevice();
        constexpr ui64 pageCount = 3000;

        {
            auto store = OpenTestStore(device, pageCount);
            Write(store, 1, "one");
            for (ui64 key = 2; key <= 2500; ++key) {
                Write(store, key, "x");
            }
            Write(store, 2501, "last");
        }

        auto buffers = Reopen(device, pageCount);
        UNIT_ASSERT_VALUES_EQUAL(2501, buffers.size());
        UNIT_ASSERT_VALUES_EQUAL("one", Get(buffers, 1));
        UNIT_ASSERT_VALUES_EQUAL("last", Get(buffers, 2501));
    }

    Y_UNIT_TEST(ShouldRejectAnOverlappingErase)
    {
        auto device = std::make_shared<TStuckDevice>();
        auto store = CreateTestStore(device);
        Restore(store);

        auto write1 = store->Write(1, MakeBuffer("one"));
        auto write2 = store->Write(2, MakeBuffer("two"));
        auto write3 = store->Write(3, MakeBuffer("three"));
        device->ReleaseAll();
        auto error = write1.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = write2.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = write3.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto erase = store->EraseBelow(3);
        UNIT_ASSERT_VALUES_EQUAL(1, device->PendingCount());
        UNIT_ASSERT(!erase.HasValue());

        // a second erase has to wait for the first one to complete
        error = store->EraseBelow(4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        // even a lower bound - it is not persisted until the write is done
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        // an erased key is refused right away
        error = store->Write(2, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        device->ReleaseAll();
        error = erase.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, device->PendingCount());

        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);

        auto erase3 = store->EraseBelow(4);
        device->ReleaseAll();
        error = erase3.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT(buffers.empty());
    }

    Y_UNIT_TEST(ShouldReportTheDeviceWriteError)
    {
        auto device = std::make_shared<TStuckDevice>();
        auto store = CreateTestStore(device);
        Restore(store);

        auto write = store->Write(1, MakeBuffer("one"));
        device->ReleaseAll();
        auto error = write.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        device->Broken = true;

        error = store->Write(2, MakeBuffer(TString(40, 'x'))).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, error.GetCode());
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, error.GetCode());

        device->Broken = false;

        // the pages of the failed write are free again: 5 of the 6 entry
        // pages are left after key 1
        TVector<NThreading::TFuture<NCloud::NProto::TError>> writes;
        for (ui64 key = 2; key <= 6; ++key) {
            writes.push_back(store->Write(key, MakeBuffer("x")));
        }
        device->ReleaseAll();
        for (auto& future: writes) {
            error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }
        error = store->Write(7, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        // the failed erase changed nothing, it can be retried
        auto erase = store->EraseBelow(2);
        device->ReleaseAll();
        error = erase.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("2=x|3=x|4=x|5=x|6=x", Describe(buffers));
    }
}

}   // namespace NCloud::NJournalled
