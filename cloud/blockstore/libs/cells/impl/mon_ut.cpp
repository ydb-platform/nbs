#include "mon.h"

#include <cloud/blockstore/libs/diagnostics/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NCells {

Y_UNIT_TEST_SUITE(TCellsMonTest)
{
    Y_UNIT_TEST(ShouldRenderIcV2PortForPeerHost)
    {
        NProto::TCellsConfig proto;
        proto.SetCellId("local");
        auto* peer = proto.AddCells();
        peer->SetCellId("remote");
        peer->SetIcV2Port(39602);
        peer->AddHosts()->SetFqdn("peer-host");
        const TCellsConfig config(std::move(proto));
        TCellsSnapshot snapshot;
        snapshot.HostStatuses["remote"].push_back({.Fqdn = "peer-host"});
        TStringStream out;
        RenderCellsPage(out, config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();
        UNIT_ASSERT_STRING_CONTAINS(html, ">39602</td>");
        UNIT_ASSERT(html.find("IC V2 XDC") == TString::npos);
    }

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
             .GrpcDataFallbackEnabled = true});
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
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "value='CELL_DATA_TRANSPORT_ICV2'>IC V2</option>");
        UNIT_ASSERT_STRING_CONTAINS(html, "grpc fallback</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "disk&lt;&amp;&quot;&#39;");
        UNIT_ASSERT_STRING_CONTAINS(html, "client&lt;&amp;&quot;&#39;");
        UNIT_ASSERT_STRING_CONTAINS(html, "remote&lt;&amp;&quot;&#39;");
    }

    Y_UNIT_TEST(ShouldDistinguishSelectedIcV2FromConnectedChannel)
    {
        NProto::TCellsConfig proto;
        proto.SetCellId("local");
        const TCellsConfig config(std::move(proto));
        TCellsSnapshot snapshot;
        snapshot.Mounts.push_back(
            {.DiskId = "disk",
             .ClientId = "client",
             .CellId = "remote",
             .Host = "localhost",
             .DataTransport = "icv2",
             .SelectedTransport = NProto::CELL_DATA_TRANSPORT_ICV2,
             .IcV2Channel = NCloud::NIcV2::TPeerStatus{
                 .State = NCloud::NIcV2::EPeerState::Disconnected,
                 .ConnectionAttempts = 2,
                 .ConnectionFailures = 1,
             },
             .Io = {
                 .Read = {.Requests = 3, .Errors = 1, .Bytes = 12288},
                 .Write = {.Requests = 2, .Errors = 2, .Bytes = 8192},
                 .Zero = {.Requests = 1, .Errors = 1},
             }});
        TStringStream out;
        RenderCellsPage(out, config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();
        UNIT_ASSERT_STRING_CONTAINS(html, "IC V2 channel</th>");
        UNIT_ASSERT_STRING_CONTAINS(html, "icv2</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "disconnected</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "attempts: 2, failures: 1");
        UNIT_ASSERT_STRING_CONTAINS(html, "I/O (current transport)</th>");
        UNIT_ASSERT_STRING_CONTAINS(html, "R: 3/1 (12.00 KiB)");
        UNIT_ASSERT_STRING_CONTAINS(html, "W: 2/2 (8.00 KiB)");
        UNIT_ASSERT_STRING_CONTAINS(html, "Z: 1/1");
        UNIT_ASSERT_STRING_CONTAINS(html, "label-success'>icv2</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "label-warning'>disconnected</span>");
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
        UNIT_ASSERT_STRING_CONTAINS(html, "CELL_DATA_TRANSPORT_ICV2' disabled");
        UNIT_ASSERT_STRING_CONTAINS(html, "Requires gRPC data fallback");
    }

    Y_UNIT_TEST(ShouldOfferIcV2NextToRdma)
    {
        const TCellsConfig config(NProto::TCellsConfig{});
        TCellsSnapshot snapshot;
        snapshot.Mounts.push_back({
            .ConnectionId = "connection",
            .DiskId = "disk",
            .ClientId = "client",
            .CellId = "remote",
            .Host = "localhost",
            .DataTransport = "icv2",
            .SelectedTransport = NProto::CELL_DATA_TRANSPORT_ICV2,
            .GrpcDataFallbackEnabled = true,
        });
        TStringStream out;
        RenderCellsPage(out, config, snapshot, TDiagnosticsConfig());
        const auto html = out.Str();
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "value='CELL_DATA_TRANSPORT_ICV2' selected>IC V2</option>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<span class='label label-success'>icv2</span>");
    }
}

}   // namespace NCloud::NBlockStore::NCells
