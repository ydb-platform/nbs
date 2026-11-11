#include "mon.h"

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NCells {

Y_UNIT_TEST_SUITE(TCellsMonTest)
{
    Y_UNIT_TEST(ShouldRenderTransportSelectionForEachMount)
    {
        NProto::TCellsConfig proto;
        proto.SetCellId("local");
        const TCellsConfig config(std::move(proto));
        TCellsSnapshot snapshot;
        snapshot.Mounts.push_back(
            {.ConnectionId = "connection<&\"'",
             .DiskId = "disk<&\"'",
             .ClientId = "client<&\"'",
             .CellId = "remote<&\"'",
             .Host = "localhost",
             .DataTransport = "grpc fallback",
             .TabletHost = "localhost",
             .SelectedTransport = NProto::CELL_DATA_TRANSPORT_RDMA,
             .CanSelectRdma = true});
        TStringStream out;
        RenderCellsPage(out, config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();
        UNIT_ASSERT_STRING_CONTAINS(html, "<form method='post' action='cells'");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "name='action' value='switchTransport'");
        UNIT_ASSERT_STRING_CONTAINS(html, "<select name='Transport'");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "name='ConnectionId' value='connection&lt;&amp;&quot;&#39;'");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "value='CELL_DATA_TRANSPORT_GRPC'>gRPC</option>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "value='CELL_DATA_TRANSPORT_RDMA' selected>RDMA</option>");
        UNIT_ASSERT_STRING_CONTAINS(html, "grpc fallback</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "disk&lt;&amp;&quot;&#39;");
        UNIT_ASSERT_STRING_CONTAINS(html, "client&lt;&amp;&quot;&#39;");
        UNIT_ASSERT_STRING_CONTAINS(html, "remote&lt;&amp;&quot;&#39;");
    }
    Y_UNIT_TEST(ShouldDisableManualRdmaSelectionWithoutFallback)
    {
        const TCellsConfig config(NProto::TCellsConfig{});
        TCellsSnapshot snapshot;
        snapshot.Mounts.push_back({
            .ConnectionId = "connection",
            .DiskId = "disk",
            .ClientId = "client",
            .CellId = "remote",
            .Host = "localhost",
            .DataTransport = "grpc",
            .SelectedTransport = NProto::CELL_DATA_TRANSPORT_GRPC,
        });
        TStringStream out;
        RenderCellsPage(out, config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "name='ConnectionId' value='connection'");
        UNIT_ASSERT_STRING_CONTAINS(html, "CELL_DATA_TRANSPORT_RDMA' disabled");
        UNIT_ASSERT_STRING_CONTAINS(html, "Requires gRPC data fallback");
    }
}

}   // namespace NCloud::NBlockStore::NCells
