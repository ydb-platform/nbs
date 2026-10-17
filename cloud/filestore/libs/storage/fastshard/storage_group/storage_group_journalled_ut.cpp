#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_quorum.h>

#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/fastshard/protos/device.pb.h>
#include <cloud/fastshard/testlib/fiber_test.h>
#include <cloud/fastshard/testlib/journalled_storage_node.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

#include <gtest/gtest.h>

#include <initializer_list>

using namespace NCloud;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Content(ui64 page)
{
    TString content = TStringBuilder() << "page" << page;
    content.resize(DefaultBlockSize, 'a' + page % 26);
    return content;
}

TVector<TString> Pages(ui64 from, ui64 to)
{
    TVector<TString> pages;
    for (ui64 page = from; page < to; ++page) {
        pages.push_back(Content(page));
    }
    return pages;
}

NProto::TError WritePages(IStorageGroup& group, ui64 from, ui64 to)
{
    for (ui64 page = from; page < to; ++page) {
        const TString content = Content(page);
        TPageGroup pageGroup{.FirstPageNo = page};
        pageGroup.Content.emplace_back(content.data(), content.size());
        TVector<TPageGroup> pageGroups;
        pageGroups.push_back(std::move(pageGroup));

        auto error = group.WriteLogRecord(
            {},
            std::move(pageGroups),
            {.Lsn = page, .PrevLsn = page - 1});
        if (HasError(error)) {
            return error;
        }
    }
    return {};
}

NProto::TError ReadPages(
    IStorageGroup& group,
    ui64 from,
    ui64 to,
    TVector<TString>& pages)
{
    TVector<TPageGroupRef> refs = {{.FirstPageNo = from, .PageCount = to - from}};
    TVector<TPageGroup> pageGroups;
    auto error = group.ReadPages({}, refs, &pageGroups);
    if (HasError(error)) {
        return error;
    }

    pages.clear();
    for (const auto& pageGroup: pageGroups) {
        for (const auto& content: pageGroup.Content) {
            pages.emplace_back(content.Data(), content.Size());
        }
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

// Journalled devices on an executor of their own. A test opens groups over
// any of them, and is to tear a group down before the devices go.
struct TJournalledDevices
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    TExecutorPtr Executor = TExecutor::Create("journalled-sn");
    TVector<TString> DeviceUUIDs;
    TVector<std::shared_ptr<TJournalledStorageNode>> Nodes;

    TJournalledDevices(ui32 count)
    {
        Executor->Start();
        for (ui32 i = 0; i < count; ++i) {
            DeviceUUIDs.push_back(TStringBuilder() << "dev-" << char('a' + i));
            Nodes.push_back(std::make_shared<TJournalledStorageNode>(
                DeviceUUIDs[i],
                Logging,
                Executor));
            Nodes[i]->Start();
        }
    }

    // The nodes run on the executor, so they stop before it does. The
    // executor is joined here: a flush cycle may still hold the last device
    // reference, and dropping it on the executor would join that thread
    // from itself.
    ~TJournalledDevices()
    {
        for (auto& node: Nodes) {
            node->Stop();
        }
        Executor->Stop();
    }

    TJournalledStorageNode& operator[](ui32 i)
    {
        return *Nodes[i];
    }

    // The background loops are off unless a test asks for one.
    IStorageGroupPtr MakeGroup(
        std::initializer_list<ui32> indexes,
        TDuration lowWatermarkPeriod = TDuration::Zero())
    {
        TVector<TStorageDevice> devices;
        for (ui32 i: indexes) {
            devices.push_back({.Node = Nodes[i], .DeviceUUID = DeviceUUIDs[i]});
        }

        TStorageGroupConfig config;
        config.LowWatermarkPeriod = lowWatermarkPeriod;
        config.ReacquirePeriod = TDuration::Zero();
        return CreateQuorumMirroredStorageGroup(
            std::move(config),
            std::move(devices),
            CreateFiberTimer());
    }

    // A device's own page, before the group's page shift.
    NProto::TError ReadPage(ui32 i, ui64 pageNo, TString& page)
    {
        NProto::TReadPagesRequest request;
        request.SetDeviceUUID(DeviceUUIDs[i]);
        auto* ref = request.AddPageGroupRefs();
        ref->SetFirstPageNo(pageNo);
        ref->SetPageCount(1);
        ref->SetPageSize(Nodes[i]->Layout.PageSize);

        auto response = Nodes[i]->Device->ReadPages(request).GetValueSync();
        if (HasError(response.GetError())) {
            return response.GetError();
        }

        page = response.GetPageGroups(0).GetContent(0);
        return {};
    }

    // A record on the device itself, behind the group's back.
    NProto::TError WritePage(ui32 i, ui64 lsn, ui64 pageNo, TString page)
    {
        NProto::TWriteLogRecordRequest request;
        request.SetDeviceUUID(DeviceUUIDs[i]);
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(lsn - 1);

        auto* pg = request.AddPageGroups();
        pg->SetFirstPageNo(pageNo);
        *pg->AddContent() = std::move(page);

        return Nodes[i]->Device->WriteLogRecord(request).GetValueSync().GetError();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

FIBER_TEST(JournalledGroupTest, WritesSurviveARestart)
{
    TJournalledDevices devices(3);
    auto group = devices.MakeGroup({0, 1, 2});
    auto init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    ASSERT_EQ(1U, init.GetResult());

    auto error = WritePages(*group, 2, 7);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    TVector<TString> pages;
    error = ReadPages(*group, 2, 7, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 7), pages);
    group->TearDown();

    group = devices.MakeGroup({0, 1, 2});
    init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    ASSERT_EQ(6U, init.GetResult());

    error = ReadPages(*group, 2, 7, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 7), pages);

    error = WritePages(*group, 7, 9);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    error = ReadPages(*group, 2, 9, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 9), pages);

    group->TearDown();
}

FIBER_TEST(JournalledGroupTest, ABlankDeviceCatchesUpAndServesAlone)
{
    TJournalledDevices devices(2);
    auto group = devices.MakeGroup({0});
    auto init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();

    auto error = WritePages(*group, 2, 5);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    group->TearDown();

    // A blank device joins: it is claimed and brought up with the records.
    group = devices.MakeGroup({0, 1});
    init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    ASSERT_EQ(4U, init.GetResult());
    group->TearDown();

    // What it holds is enough on its own.
    group = devices.MakeGroup({1});
    init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    ASSERT_EQ(4U, init.GetResult());

    TVector<TString> pages;
    error = ReadPages(*group, 2, 5, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 5), pages);

    group->TearDown();
}

FIBER_TEST(JournalledGroupTest, ADamagedFirstPageFailsInit)
{
    TJournalledDevices devices(3);
    auto group = devices.MakeGroup({0, 1, 2});
    auto init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();

    auto error = WritePages(*group, 2, 5);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    group->TearDown();

    // Garbage where the claim was.
    error = devices.WritePage(0, 5, 0, TString(DefaultBlockSize, 'x'));
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    group = devices.MakeGroup({0, 1, 2});
    init = group->Init();
    ASSERT_EQ(E_INVALID_STATE, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_TRUE(init.GetError().GetMessage().Contains(devices.DeviceUUIDs[0]))
        << init.GetError().GetMessage();
    group->TearDown();

    // Another device's claim there is no better.
    TString claim;
    error = devices.ReadPage(1, 0, claim);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    error = devices.WritePage(0, 6, 0, claim);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    group = devices.MakeGroup({0, 1, 2});
    init = group->Init();
    ASSERT_EQ(E_INVALID_STATE, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_TRUE(init.GetError().GetMessage().Contains(devices.DeviceUUIDs[0]))
        << init.GetError().GetMessage();
    group->TearDown();
}

FIBER_TEST(JournalledGroupTest, ADeadDeviceFailsInit)
{
    TJournalledDevices devices(3);
    devices[2].Stop();

    auto group = devices.MakeGroup({0, 1, 2});
    const auto init = group->Init();

    ASSERT_TRUE(HasError(init.GetError()));
    ASSERT_TRUE(init.GetError().GetMessage().Contains(devices.DeviceUUIDs[2]))
        << init.GetError().GetMessage();

    group->TearDown();
}

FIBER_TEST(JournalledGroupTest, RunsWithTheWatermarkLoop)
{
    TJournalledDevices devices(3);
    const auto period = TDuration::MilliSeconds(1);

    // The loop trims what every device holds as it goes; whatever it has
    // trimmed by the time the group restarts, nothing is lost.
    auto group = devices.MakeGroup({0, 1, 2}, period);
    auto init = group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();

    auto error = WritePages(*group, 2, 21);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    TVector<TString> pages;
    error = ReadPages(*group, 2, 21, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 21), pages);
    group->TearDown();

    group = devices.MakeGroup({0, 1, 2}, period);
    init = group->Init();

    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    ASSERT_EQ(20U, init.GetResult());
    error = ReadPages(*group, 2, 21, pages);

    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 21), pages);

    error = WritePages(*group, 21, 23);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    error = ReadPages(*group, 2, 23, pages);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(Pages(2, 23), pages);
    group->TearDown();
}
