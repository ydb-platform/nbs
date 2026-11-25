#include "filesystem_event.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestHandler final
    : public IFileSystemEventHandler
{
    ui32 EventCount = 0;
    TVector<ui64> DisconnectedSourceIds;

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        Y_UNUSED(event);
        ++EventCount;
    }

    void OnDisconnect(ui64 sourceId) override
    {
        DisconnectedSourceIds.push_back(sourceId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMultiFileSystemEventHandlerTest)
{
    Y_UNIT_TEST(ShouldDeliverEventsByFileSystemAndDisconnectsToAll)
    {
        auto multiHandler = CreateMultiFileSystemEventHandler();
        auto handler1 = std::make_shared<TTestHandler>();
        auto handler2 = std::make_shared<TTestHandler>();
        multiHandler->Register("fs1", handler1);
        multiHandler->Register("fs2", handler2);

        NProto::TFileSystemEvent event;
        event.SetFileSystemId("fs1");
        multiHandler->OnEvent(event);
        UNIT_ASSERT_VALUES_EQUAL(1, handler1->EventCount);
        UNIT_ASSERT_VALUES_EQUAL(0, handler2->EventCount);

        multiHandler->OnDisconnect(42 /* sourceId */);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>{42},
            handler1->DisconnectedSourceIds);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>{42},
            handler2->DisconnectedSourceIds);
    }
}

}   // namespace NCloud::NFileStore
