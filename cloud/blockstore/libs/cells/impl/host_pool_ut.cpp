#include "host_pool.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

TCellConfigPtr MakeCellConfig()
{
    NProto::TCellConfig proto;
    proto.SetCellId("cell-1");
    proto.SetGrpcPort(9766);
    proto.SetRdmaPort(10020);
    proto.SetTransport(NProto::CELL_DATA_TRANSPORT_RDMA);
    proto.AddHosts()->SetFqdn("host-a");
    proto.AddHosts()->SetFqdn("host-b");

    return std::make_shared<TCellConfig>(std::move(proto));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellHostPoolTest)
{
    Y_UNIT_TEST(ShouldMakeHostConfigForUnlistedFqdn)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        // the tablet host arrives at runtime and need not be in the config;
        // the ports are cell-wide, so the fqdn alone is enough
        auto host = pool.MakeHostConfig("host-z");
        UNIT_ASSERT_VALUES_EQUAL("host-z", host.GetFqdn());
        UNIT_ASSERT_VALUES_EQUAL(9766, host.GetGrpcPort());
        UNIT_ASSERT_VALUES_EQUAL(10020, host.GetRdmaPort());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::CELL_DATA_TRANSPORT_RDMA),
            static_cast<int>(host.GetTransport()));
    }

    Y_UNIT_TEST(ShouldPickOnlyConfiguredHosts)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool.PickConfiguredHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());

            const auto& fqdn = picked.GetResult().GetFqdn();
            UNIT_ASSERT_C(
                fqdn == "host-a" || fqdn == "host-b",
                "unexpected host " + fqdn);
        }
    }

    Y_UNIT_TEST(ShouldNotPickDeadHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        pool.SetHostAlive("host-a", false);
        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool.PickConfiguredHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL("host-b", picked.GetResult().GetFqdn());
        }

        pool.SetHostAlive("host-b", false);
        auto picked = pool.PickConfiguredHost();
        UNIT_ASSERT(HasError(picked));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, picked.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReviveHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);
        UNIT_ASSERT(HasError(pool.PickConfiguredHost()));

        pool.SetHostAlive("host-a", true);
        auto picked = pool.PickConfiguredHost();
        UNIT_ASSERT_C(!HasError(picked), picked.GetError());
        UNIT_ASSERT_VALUES_EQUAL("host-a", picked.GetResult().GetFqdn());
    }

    Y_UNIT_TEST(ShouldIgnoreLivenessOfUnlistedHost)
    {
        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        // marking a host we have never heard of must not resurrect it into
        // the configured population
        pool.SetHostAlive("host-z", true);
        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);

        UNIT_ASSERT(HasError(pool.PickConfiguredHost()));
    }
}

}   // namespace NCloud::NBlockStore::NCells
