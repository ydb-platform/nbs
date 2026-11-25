#include <cloud/filestore/libs/storage/api/components.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileStoreComponentsTest)
{
    // A failure here could mean that the wiring of events could have been
    // affected and might lead to compatibility issues
    Y_UNIT_TEST(ShouldKeepFileStoreEventsPrivateRangesStable)
    {
        using E = TFileStoreEventsPrivate;

        UNIT_ASSERT_VALUES_EQUAL(E::START + 1, (int)E::SCHEMESHARD_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 101, (int)E::SCHEMESHARD_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 102, (int)E::SERVICE_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 202, (int)E::SERVICE_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 203, (int)E::SERVICE_WORKER_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 303, (int)E::SERVICE_WORKER_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 304, (int)E::SERVICE_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 404, (int)E::SERVICE_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 405, (int)E::TABLET_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 505, (int)E::TABLET_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 506, (int)E::TABLET_WORKER_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 606, (int)E::TABLET_WORKER_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 607, (int)E::TABLET_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 707, (int)E::TABLET_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 708, (int)E::SS_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 808, (int)E::SS_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(
            E::START + 809,
            (int)E::DISK_REGISTRY_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(
            E::START + 909,
            (int)E::DISK_REGISTRY_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 910, (int)E::RDMA_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 1010, (int)E::RDMA_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 1011, (int)E::END);
    }

    // A failure here could mean that the wiring of events could have been
    // affected and might lead to compatibility issues
    Y_UNIT_TEST(ShouldKeepFileStoreEventsRangesStable)
    {
        using E = TFileStoreEvents;

        UNIT_ASSERT_VALUES_EQUAL(E::START + 1, (int)E::SCHEMESHARD_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 101, (int)E::SCHEMESHARD_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 102, (int)E::SERVICE_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 202, (int)E::SERVICE_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 203, (int)E::SERVICE_WORKER_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 303, (int)E::SERVICE_WORKER_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 304, (int)E::SERVICE_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 404, (int)E::SERVICE_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 405, (int)E::TABLET_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 505, (int)E::TABLET_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 506, (int)E::TABLET_WORKER_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 606, (int)E::TABLET_WORKER_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 607, (int)E::TABLET_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 707, (int)E::TABLET_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 708, (int)E::SS_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 808, (int)E::SS_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(
            E::START + 809,
            (int)E::DISK_REGISTRY_PROXY_START);
        UNIT_ASSERT_VALUES_EQUAL(
            E::START + 909,
            (int)E::DISK_REGISTRY_PROXY_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 910, (int)E::SERVICE_EXTRA_START);
        UNIT_ASSERT_VALUES_EQUAL(E::START + 1010, (int)E::SERVICE_EXTRA_END);

        UNIT_ASSERT_VALUES_EQUAL(E::START + 1011, (int)E::END);
    }
}

}   // namespace NCloud::NFileStore::NStorage
