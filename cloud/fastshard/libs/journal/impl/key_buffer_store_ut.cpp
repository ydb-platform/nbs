#include "key_buffer_store.h"

#include "memory_device.h"

#include <cloud/fastshard/libs/journal/iface/device.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// 24 bytes of page header, 24 bytes of entry header and 24 bytes of payload
// per page, 40 bytes of header and 32 bytes of metadata per superblock page
constexpr ui32 TestPageSize = 72;
constexpr ui32 TestEntryCapacity = 24;
constexpr ui32 TestMetaCapacity = 32;
constexpr ui64 TestPageCount = 8;   // 2 superblock slots and 6 entry pages
constexpr ui32 TestMinBufferSize = 16;

// a page several entries share: 24 bytes of page header and 27 bytes per entry
// of a 3-byte payload, the 7th entry leaves less than the 40 bytes of the
// smallest entry
constexpr ui32 TestWidePageSize = 200;
constexpr ui64 TestWidePageEntryCount = 6;

TBuffer MakeBuffer(TStringBuf data)
{
    return TBuffer(data.data(), data.size());
}

TString AsString(const TBuffer& buffer)
{
    return TString(buffer.Data(), buffer.Size());
}

using TKeyBuffers = TVector<TKeyBuffer>;

TString Get(const TKeyBuffers& buffers, ui64 key)
{
    auto it = FindIf(buffers, [key](const auto& entry) {
        return entry.Key == key;
    });
    UNIT_ASSERT_C(it != buffers.end(), "key " << key << " is missing");
    return AsString(it->Buffer);
}

TRestoredBuffers RestoreAll(const IKeyBufferStorePtr& store)
{
    auto response = store->Restore().GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        response.GetError().GetCode(),
        FormatError(response.GetError()));
    return response.ExtractResult();
}

TKeyBuffers Restore(const IKeyBufferStorePtr& store)
{
    return RestoreAll(store).KeyBuffers;
}

// the metadata a fresh store instance restores from the store
TString RestoreMeta(const IKeyBufferStorePtr& store)
{
    return AsString(RestoreAll(store).MetaBuffer);
}

// "<key>=<buffer>|..." in the key order
TString Describe(TKeyBuffers buffers)
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

void WriteMeta(const IKeyBufferStorePtr& store, TStringBuf data)
{
    const auto error = store->WriteMeta(MakeBuffer(data)).GetValueSync();
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
    static const ILoggingServicePtr logging = [] {
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
    ui64 pageCount = TestPageCount,
    ui32 pageSize = TestPageSize)
{
    return CreateDeviceKeyBufferStore(
        TestLogging(),
        device,
        pageCount,
        pageSize,
        TestMinBufferSize);
}

// a store instance restored from the device
IKeyBufferStorePtr OpenTestStore(
    const IDevicePtr& device,
    ui64 pageCount = TestPageCount,
    ui32 pageSize = TestPageSize)
{
    auto store = CreateTestStore(device, pageCount, pageSize);
    Restore(store);
    return store;
}

// what a fresh store instance restores from the device
TKeyBuffers Reopen(
    const IDevicePtr& device,
    ui64 pageCount = TestPageCount,
    ui32 pageSize = TestPageSize)
{
    return Restore(CreateTestStore(device, pageCount, pageSize));
}

TString ReadFromDevice(const IDevicePtr& device, ui64 pageNo)
{
    const auto result =
        device->ReadPages({{.FirstPageNo = pageNo, .PageCount = 1}})
            .GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        result.GetError().GetCode(),
        FormatError(result.GetError()));
    UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());

    const auto& page = result.GetResult()[0];
    return TString(page.Data(), page.Size());
}

void WriteToDevice(const IDevicePtr& device, ui64 pageNo, TString content)
{
    TPageRange range{.FirstPageNo = pageNo};
    range.Pages.emplace_back(content.data(), content.size());

    const auto error =
        device->WritePages({std::move(range)}).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
}

// the entry page holding |payload|, which the store picks on its own
ui64 FindDevicePage(
    const IDevicePtr& device,
    TStringBuf payload,
    ui64 pageCount = TestPageCount)
{
    for (ui64 pageNo = 0; pageNo < pageCount; ++pageNo) {
        if (ReadFromDevice(device, pageNo).Contains(payload)) {
            return pageNo;
        }
    }
    UNIT_FAIL("no page holds " << payload);
    return pageCount;
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
    const IDevicePtr Device = CreateInMemoryDevice(TestPageSize);

    TVector<std::function<void()>> Pending;

public:
    bool Broken = false;

    NThreading::TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        return Device->ReadPages(std::move(rangeRefs));
    }

    NThreading::TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        if (Broken) {
            return NThreading::MakeFuture(
                MakeError(E_IO, "device is broken"));
        }

        auto promise = NThreading::NewPromise<NCloud::NProto::TError>();

        Pending.push_back(
            [device = Device, ranges = std::move(ranges), promise]() mutable
            {
                promise.SetValue(
                    device->WritePages(std::move(ranges)).GetValueSync());
            });

        return promise.GetFuture();
    }

    NThreading::TFuture<NCloud::NProto::TError> ZeroPages(
        TVector<TPageRangeRef> ranges) override
    {
        if (Broken) {
            return NThreading::MakeFuture(
                MakeError(E_IO, "device is broken"));
        }

        return Device->ZeroPages(std::move(ranges));
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

    Y_UNIT_TEST(ShouldOverwriteExistingKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "first");
        Write(store, 1, "second");

        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("1=second", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepMetadata)
    {
        auto store = CreateInMemoryKeyBufferStore();

        UNIT_ASSERT_VALUES_EQUAL("", RestoreMeta(store));

        WriteMeta(store, "alpha");
        UNIT_ASSERT_VALUES_EQUAL("alpha", RestoreMeta(store));

        // it is replaced as a whole and survives an erase
        WriteMeta(store, "b");
        Write(store, 1, "one");
        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        UNIT_ASSERT_VALUES_EQUAL("b", RestoreMeta(store));
    }

    Y_UNIT_TEST(ShouldEraseSingleKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 1, "one");

        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());

        // a bound that does not move reports that nothing was done
        code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
    }

    Y_UNIT_TEST(ShouldEraseEveryKeyBelowGivenOne)
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

    Y_UNIT_TEST(ShouldEraseNothingBelowLowestKey)
    {
        auto store = CreateInMemoryKeyBufferStore();

        Write(store, 5, "x");

        // the bound moves, so it is not a no-op, the key stays
        auto code = EraseBelow(store, 5);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        auto buffers = Restore(store);
        UNIT_ASSERT_VALUES_EQUAL("5=x", Describe(buffers));

        // and on an empty store
        code = EraseBelow(store, 6);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
        code = EraseBelow(store, Max<ui64>());
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

    Y_UNIT_TEST(ShouldRefuseToWriteErasedKey)
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

    Y_UNIT_TEST(ShouldKeepIndependentCopyOfBuffer)
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
    Y_UNIT_TEST(ShouldStartEmptyOnFreshDevice)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
        auto store = CreateTestStore(device);

        auto buffers = Restore(store);
        UNIT_ASSERT(buffers.empty());
        buffers = Reopen(device);
        UNIT_ASSERT(buffers.empty());
    }

    Y_UNIT_TEST(ShouldRequireRestoreBeforeUse)
    {
        auto store = CreateTestStore(CreateInMemoryDevice(TestPageSize));

        auto error = store->Write(1, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
        error = store->WriteMeta(MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
    }

    Y_UNIT_TEST(ShouldKeepBuffersAcrossRestores)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, "two");
            Write(store, 3, "");
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=one|2=two|3=", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldNotTouchPagesBeyondDevice)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        auto store = OpenTestStore(device);
        Write(store, 1, TString(TestEntryCapacity, 'a'));
        Write(store, 2, "x");
        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        const bool isZero = IsZeroDevicePage(device, TestPageCount);
        UNIT_ASSERT(isZero);
    }

    Y_UNIT_TEST(ShouldRejectBufferThatDoesNotFit)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
        auto store = OpenTestStore(device);

        // an entry never spans pages, so the page bounds the buffer
        auto error =
            store->Write(1, MakeBuffer(TString(TestEntryCapacity + 1, 'x')))
                .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        const TString largest(TestEntryCapacity, 'x');
        Write(store, 1, largest);

        // the rejected buffer has taken nothing - the other 5 pages are free
        for (ui64 key = 2; key <= 6; ++key) {
            Write(store, key, "x");
        }
        error = store->Write(7, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(
            "1=" + largest + "|2=x|3=x|4=x|5=x|6=x",
            Describe(buffers));
    }

    Y_UNIT_TEST(ShouldReusePagesOfErasedBuffers)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
        auto store = OpenTestStore(device);

        for (ui64 key = 1; key <= 6; ++key) {
            Write(store, key, "x");
        }

        auto error = store->Write(7, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto code = EraseBelow(store, 4);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        // the 3 pages of the erased keys take 3 more entries
        const TString largest(TestEntryCapacity, 'y');
        Write(store, 7, largest);
        Write(store, 8, "x");
        Write(store, 9, "x");

        error = store->Write(10, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(
            "4=x|5=x|6=x|7=" + largest + "|8=x|9=x",
            Describe(buffers));
    }

    Y_UNIT_TEST(ShouldPersistErasedBound)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

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

    Y_UNIT_TEST(ShouldKeepNewestCopyOfRewrittenKey)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

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

    Y_UNIT_TEST(ShouldHoldPagesOfOverwrittenCopiesUntilErased)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
        auto store = OpenTestStore(device);

        // a page each time, 6 in the store - the old copies stay behind
        for (int i = 0; i < 6; ++i) {
            const auto fill = static_cast<char>('a' + i);
            Write(store, 1, TString(TestEntryCapacity, fill));
        }

        auto error = store->Write(1, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(
            "1=" + TString(TestEntryCapacity, 'f'),
            Describe(buffers));

        // the erase releases every copy
        auto code = EraseBelow(store, 2);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        for (ui64 key = 2; key <= 7; ++key) {
            Write(store, key, "x");
        }

        buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("2=x|3=x|4=x|5=x|6=x|7=x", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldPackSeveralEntriesIntoPage)
    {
        // 2 superblock slots and 3 entry pages - an append copies the page
        // into a free one, so the store needs a free page beside the two in
        // use
        constexpr ui64 pageCount = 5;

        auto device = CreateInMemoryDevice(TestWidePageSize);
        auto store = OpenTestStore(device, pageCount, TestWidePageSize);

        // 6 entries fill the first page, 6 more the second, the 13th starts
        // the third - every append copies the entries written so far, which
        // must come out intact
        TStringBuilder expected;
        for (ui64 key = 1; key <= 2 * TestWidePageEntryCount + 1; ++key) {
            const TString payload(3, static_cast<char>('a' + key));
            Write(store, key, payload);

            if (expected) {
                expected << "|";
            }
            expected << key << "=" << payload;
        }

        // the third page has room, but no free page is left to append into
        auto error = store->Write(100, MakeBuffer("xyz")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        auto buffers = Reopen(device, pageCount, TestWidePageSize);
        UNIT_ASSERT_VALUES_EQUAL(expected, Describe(buffers));
    }

    Y_UNIT_TEST(ShouldDropErasedEntriesFromAppendablePage)
    {
        constexpr ui64 pageCount = 5;

        auto device = CreateInMemoryDevice(TestWidePageSize);
        auto store = OpenTestStore(device, pageCount, TestWidePageSize);

        Write(store, 1, "aaa");
        Write(store, 2, "bbb");
        Write(store, 3, "ccc");

        auto code = EraseBelow(store, 3);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, code);

        // the page straddles the bound, so it stays - the next append leaves
        // the erased entries behind
        Write(store, 4, "ddd");

        const auto page =
            ReadFromDevice(device, FindDevicePage(device, "ddd", pageCount));
        UNIT_ASSERT(page.Contains("ccc"));
        UNIT_ASSERT(!page.Contains("aaa"));
        UNIT_ASSERT(!page.Contains("bbb"));

        auto buffers = Reopen(device, pageCount, TestWidePageSize);
        UNIT_ASSERT_VALUES_EQUAL("3=ccc|4=ddd", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepNewestCopyOfKeyRewrittenWithinPage)
    {
        constexpr ui64 pageCount = 5;

        auto device = CreateInMemoryDevice(TestWidePageSize);
        auto store = OpenTestStore(device, pageCount, TestWidePageSize);

        Write(store, 1, "aaa");
        Write(store, 2, "bbb");
        Write(store, 1, "AAA");

        // the rewrite appends to the page - the old copy stays in it until
        // an erase takes it out
        const auto page =
            ReadFromDevice(device, FindDevicePage(device, "AAA", pageCount));
        UNIT_ASSERT(page.Contains("aaa"));
        UNIT_ASSERT(page.Contains("bbb"));

        auto buffers = Reopen(device, pageCount, TestWidePageSize);
        UNIT_ASSERT_VALUES_EQUAL("1=AAA|2=bbb", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldDropTornBuffer)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        const TString torn(TestEntryCapacity, 'b');

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "one");
            Write(store, 2, torn);
            Write(store, 3, "three");
        }

        CorruptDevicePage(device, FindDevicePage(device, torn));

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=one|3=three", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldKeepOldCopyWhenRewriteIsTorn)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        const TString torn(TestEntryCapacity, 's');

        {
            auto store = OpenTestStore(device);
            Write(store, 1, "first");
            Write(store, 1, torn);
        }

        CorruptDevicePage(device, FindDevicePage(device, torn));

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL("1=first", Describe(buffers));
    }

    Y_UNIT_TEST(ShouldSurviveTornSuperblock)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

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

    Y_UNIT_TEST(ShouldKeepMetadataAcrossRestores)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        {
            auto store = OpenTestStore(device);
            UNIT_ASSERT_VALUES_EQUAL("", RestoreMeta(CreateTestStore(device)));

            Write(store, 1, "one");
            Write(store, 2, "two");
            WriteMeta(store, "alpha");
        }

        {
            auto store = CreateTestStore(device);
            auto restored = RestoreAll(store);
            UNIT_ASSERT_VALUES_EQUAL("alpha", AsString(restored.MetaBuffer));
            UNIT_ASSERT_VALUES_EQUAL(
                "1=one|2=two",
                Describe(restored.KeyBuffers));

            // an erase keeps the metadata, a metadata write keeps the bound
            auto code = EraseBelow(store, 2);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, code);
            WriteMeta(store, "beta");
            code = EraseBelow(store, 2);
            UNIT_ASSERT_VALUES_EQUAL(S_FALSE, code);
        }

        auto restored = RestoreAll(CreateTestStore(device));
        UNIT_ASSERT_VALUES_EQUAL("beta", AsString(restored.MetaBuffer));
        UNIT_ASSERT_VALUES_EQUAL("2=two", Describe(restored.KeyBuffers));

        // the superblock may be emptied of metadata again
        {
            auto store = OpenTestStore(device);
            WriteMeta(store, "");
        }
        UNIT_ASSERT_VALUES_EQUAL("", RestoreMeta(CreateTestStore(device)));
    }

    Y_UNIT_TEST(ShouldRejectMetadataThatDoesNotFitSuperblock)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
        auto store = OpenTestStore(device);

        auto error = store->WriteMeta(
            MakeBuffer(TString(TestMetaCapacity + 1, 'm'))).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        const TString largest(TestMetaCapacity, 'm');
        WriteMeta(store, largest);
        UNIT_ASSERT_VALUES_EQUAL(largest, RestoreMeta(CreateTestStore(device)));
    }

    Y_UNIT_TEST(ShouldKeepPreviousMetadataWhenWriteIsTorn)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

        {
            auto store = OpenTestStore(device);
            WriteMeta(store, "alpha");   // slot 0
            WriteMeta(store, "beta");   // slot 1
        }

        // the checksum guards the metadata, not only the header
        CorruptDevicePage(device, 1, 40);

        UNIT_ASSERT_VALUES_EQUAL("alpha", RestoreMeta(CreateTestStore(device)));
    }

    Y_UNIT_TEST(ShouldIgnoreGarbageOnDevice)
    {
        auto device = CreateInMemoryDevice(TestPageSize);

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
        auto device = CreateInMemoryDevice(TestPageSize);
        WriteToDevice(device, 0, TString(TestPageSize, 'g'));
        WriteToDevice(device, 1, TString(TestPageSize, 'g'));

        auto store = CreateTestStore(device);
        auto response = store->Restore().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldRestoreLargeRangeInSeveralReads)
    {
        auto device = CreateInMemoryDevice(TestPageSize);
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

    Y_UNIT_TEST(ShouldRejectOverlappingErase)
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

        // and so does a metadata write, it shares the superblock
        error = store->WriteMeta(MakeBuffer("m")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        // a key below the pending bound is taken, the write is dropped once
        // the erase lands
        auto write = store->Write(2, MakeBuffer("x"));
        UNIT_ASSERT_VALUES_EQUAL(2, device->PendingCount());
        UNIT_ASSERT(!write.HasValue());

        device->ReleaseAll();
        error = erase.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = write.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
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

    Y_UNIT_TEST(ShouldEraseKeyWrittenBeforeOverlappingEraseLands)
    {
        auto device = std::make_shared<TStuckDevice>();
        auto store = CreateTestStore(device);
        Restore(store);

        auto write1 = store->Write(1, MakeBuffer("one"));
        device->ReleaseAll();
        auto error = write1.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        // the write lands before the erase - it succeeds and is erased
        auto write2 = store->Write(2, MakeBuffer("two"));
        auto erase = store->EraseBelow(3);
        UNIT_ASSERT_VALUES_EQUAL(2, device->PendingCount());

        device->ReleaseAll();
        error = write2.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = erase.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        // the pages of both keys are free again: all 6 entry pages
        TVector<NThreading::TFuture<NCloud::NProto::TError>> writes;
        for (ui64 key = 3; key <= 8; ++key) {
            writes.push_back(store->Write(key, MakeBuffer("x")));
        }
        error = store->Write(9, MakeBuffer("x")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        device->ReleaseAll();
        for (auto& future: writes) {
            error = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        auto buffers = Reopen(device);
        UNIT_ASSERT_VALUES_EQUAL(
            "3=x|4=x|5=x|6=x|7=x|8=x",
            Describe(buffers));
    }

    Y_UNIT_TEST(ShouldNotRejectNoOpEraseDuringMetadataWrite)
    {
        auto device = std::make_shared<TStuckDevice>();
        auto store = CreateTestStore(device);
        Restore(store);

        auto write = store->Write(3, MakeBuffer("three"));
        device->ReleaseAll();
        auto error = write.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto erase = store->EraseBelow(3);
        device->ReleaseAll();
        error = erase.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto meta = store->WriteMeta(MakeBuffer("m"));
        UNIT_ASSERT_VALUES_EQUAL(1, device->PendingCount());
        UNIT_ASSERT(!meta.HasValue());

        // the bound is persisted already, there is nothing to wait for
        error = store->EraseBelow(3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, error.GetCode());
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, error.GetCode());

        // a higher bound does have to wait
        error = store->EraseBelow(4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());

        device->ReleaseAll();
        error = meta.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        auto restored = RestoreAll(CreateTestStore(device));
        UNIT_ASSERT_VALUES_EQUAL("m", AsString(restored.MetaBuffer));
        UNIT_ASSERT_VALUES_EQUAL("3=three", Describe(restored.KeyBuffers));
    }

    Y_UNIT_TEST(ShouldReportDeviceWriteError)
    {
        auto device = std::make_shared<TStuckDevice>();
        auto store = CreateTestStore(device);
        Restore(store);

        auto write = store->Write(1, MakeBuffer("one"));
        device->ReleaseAll();
        auto error = write.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        device->Broken = true;

        error = store->Write(2, MakeBuffer("two")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, error.GetCode());
        error = store->EraseBelow(2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(E_IO, error.GetCode());
        error = store->WriteMeta(MakeBuffer("m")).GetValueSync();
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

        auto restored = RestoreAll(CreateTestStore(device));
        UNIT_ASSERT_VALUES_EQUAL(
            "2=x|3=x|4=x|5=x|6=x",
            Describe(restored.KeyBuffers));
        // the failed metadata write changed nothing either
        UNIT_ASSERT_VALUES_EQUAL("", AsString(restored.MetaBuffer));
    }
}

}   // namespace NCloud::NJournalled
