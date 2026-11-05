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
            {.DiskId = "disk<&\"'",
             .ClientId = "client<&\"'",
             .CellId = "remote<&\"'",
             .Host = "localhost",
             .DataTransport = "grpc fallback",
             .TabletHost = "localhost",
             .SelectedTransport = NProto::CELL_DATA_TRANSPORT_RDMA});
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
             .IcV2ChannelState = "disconnected",
             .IcV2ConnectionAttempts = 2,
             .IcV2ConnectionFailures = 1,
             .ReadRequests = 3,
             .ReadErrors = 1,
             .ReadBytes = 12288,
             .WriteRequests = 2,
             .WriteErrors = 2,
             .WriteBytes = 8192,
             .ZeroRequests = 1,
             .ZeroErrors = 1});
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
        UNIT_ASSERT(html.find("label-success'>icv2") == TString::npos);
    }
}

}   // namespace NCloud::NBlockStore::NCells
