#include "journal.h"

#include "device_page_store.h"
#include "key_buffer_store.h"
#include "memory_device.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/fastshard/journal/iface/device.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>

#include <atomic>
#include <optional>
#include <type_traits>
#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultPageSize = 4;
constexpr ui64 DefaultPageCount = 64;
constexpr ui64 DevicePageCount = 1024;

// the key the journal keeps its metadata under
constexpr ui64 MetadataKey = Max<ui64>();

// "<firstPageNo>x<pageCount>" pairs
using TGroups = TVector<std::pair<ui64 /*firstPageNo*/, ui64 /*pageCount*/>>;

////////////////////////////////////////////////////////////////////////////////

// Exactly DefaultPageSize bytes, the only size the page store accepts - the
// tag says which record wrote the page, e.g. "A010".
TString MakeContent(char tag, ui64 pageNo)
{
    return Sprintf("%c%03u", tag, static_cast<ui32>(pageNo % 1000));
}

NCloud::NProto::TWriteLogRecordRequest
MakeWriteRequest(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    request.SetLogSequenceNumber(lsn);
    request.SetPrevLogSequenceNumber(prevLsn);

    for (const auto& [firstPageNo, pageCount]: groups) {
        auto& group = *request.AddPageGroups();
        group.SetFirstPageNo(firstPageNo);

        for (ui64 i = 0; i < pageCount; ++i) {
            *group.AddContent() = MakeContent(tag, firstPageNo + i);
        }
    }

    return request;
}

NCloud::NProto::TReadPagesRequest MakeReadRequest(const TGroups& refs)
{
    NCloud::NProto::TReadPagesRequest request;

    for (const auto& [firstPageNo, pageCount]: refs) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
        ref.SetPageSize(DefaultPageSize);
    }

    return request;
}

// "10:[A010,A011] 20:[A020]"
template <typename T>
TString DescribeGroups(const T& source)
{
    TVector<TString> groups;

    for (const auto& group: source.GetPageGroups()) {
        groups.push_back(
            TStringBuilder() << group.GetFirstPageNo() << ":["
                             << JoinSeq(",", group.GetContent()) << "]");
    }

    return JoinSeq(" ", groups);
}

////////////////////////////////////////////////////////////////////////////////

// An in-memory device that can be told to fail its requests.
struct TTestDevice final: public IDevice
{
    const IDevicePtr Impl = CreateInMemoryDevice(DefaultPageSize);

    std::atomic<bool> FailReads = false;
    std::atomic<bool> FailWrites = false;

    TFuture<TResultOrError<TVector<TBuffer>>> ReadPages(
        TVector<TPageRangeRef> rangeRefs) override
    {
        if (FailReads.load()) {
            return MakeFuture<TResultOrError<TVector<TBuffer>>>(
                MakeError(E_IO, "read failed"));
        }

        return Impl->ReadPages(std::move(rangeRefs));
    }

    TFuture<NCloud::NProto::TError> WritePages(
        TVector<TPageRange> ranges) override
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "write failed"));
        }

        return Impl->WritePages(std::move(ranges));
    }
};

////////////////////////////////////////////////////////////////////////////////

// The same for the meta store - the underlying store is reachable through
// |Impl| so that a test can look at what the journal has persisted.
struct TTestKeyBufferStore final: public IKeyBufferStore
{
    const IKeyBufferStorePtr Impl = CreateInMemoryKeyBufferStore();

    std::atomic<bool> FailWrites = false;

    TFuture<TResultOrError<TVector<TKeyBuffer>>> Restore() override
    {
        return Impl->Restore();
    }

    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        if (FailWrites.load()) {
            return MakeFuture(MakeError(E_IO, "meta write failed"));
        }

        return Impl->Write(key, std::move(buffer));
    }

    TFuture<NCloud::NProto::TError> EraseBelow(ui64 key) override
    {
        return Impl->EraseBelow(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging;
    TExecutorPtr Executor;

    std::shared_ptr<TTestDevice> Device;
    std::shared_ptr<TTestKeyBufferStore> MetaStore;

    // the number of journal pages the next journal is given
    ui64 PageCount = DefaultPageCount;

    IDevicePageStorePtr DataStore;
    IJournalPtr Journal;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Logging = CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_RESOURCES});
        Logging->Start();

        Executor = TExecutor::Create("TestExecutor");
        Executor->Start();

        Device = std::make_shared<TTestDevice>();
        MetaStore = std::make_shared<TTestKeyBufferStore>();

        RecreateJournal();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Executor->Stop();
        Logging->Stop();
    }

    // Builds a journal over the device and the meta store, with a page store
    // of |PageCount| pages.
    void RecreateJournal()
    {
        DataStore = CreateDevicePageStore(Device, PageCount, DefaultPageSize);
        Journal = CreateJournal(
            Logging,
            Executor,
            MetaStore,
            DataStore,
            DevicePageCount);
    }

    // Runs |func| on the executor thread - the journal waits on the futures
    // of its stores through the executor, so its methods belong there.
    template <typename F>
    auto Run(F func) -> std::invoke_result_t<F>
    {
        std::optional<std::invoke_result_t<F>> result;
        Executor->Execute([&] { result.emplace(func()); }).GetValueSync();
        return std::move(*result);
    }

    // The response is ready only once the record has made it into the page
    // index, which a record that does not continue the chain yet has not.
    TFuture<NCloud::NProto::TWriteLogRecordResponse> WriteAsync(
        NCloud::NProto::TWriteLogRecordRequest request)
    {
        return Run([&] { return Journal->Write(std::move(request)); });
    }

    ui32 Write(NCloud::NProto::TWriteLogRecordRequest request)
    {
        return WriteAsync(std::move(request))
            .GetValueSync()
            .GetError()
            .GetCode();
    }

    ui32 WriteRecord(ui64 lsn, ui64 prevLsn, char tag, const TGroups& groups)
    {
        return Write(MakeWriteRequest(lsn, prevLsn, tag, groups));
    }

    NCloud::NProto::TReadPagesResponse ReadPages(const TGroups& refs)
    {
        return Run([&] { return Journal->Read(MakeReadRequest(refs)); })
            .GetValueSync();
    }

    // "0,1,meta" - the keys the meta store holds, a record under its prev lsn
    TString StoredKeys()
    {
        auto response = MetaStore->Impl->Restore().GetValueSync();
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));

        auto buffers = response.ExtractResult();
        SortBy(buffers, [](const auto& keyBuffer) { return keyBuffer.Key; });

        TVector<TString> keys;
        for (const auto& [key, buffer]: buffers) {
            keys.push_back(key == MetadataKey ? "meta" : ToString(key));
        }

        return JoinSeq(",", keys);
    }

    // Writes three single page records, one page each, and returns with the
    // journal holding all of them.
    void WriteThreeRecords()
    {
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(1, 0, 'A', {{10, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(2, 1, 'B', {{20, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteRecord(3, 2, 'C', {{30, 1}}));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalTest)
{
    Y_UNIT_TEST_F(ShouldRejectTheLsnReservedForTheMetadata, TFixture)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            WriteRecord(MetadataKey, 1, 'A', {{10, 1}}));
    }
}

}   // namespace NCloud::NJournalled
