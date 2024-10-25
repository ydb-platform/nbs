#include "monitoring_utils.h"

#include "config.h"
#include "probes.h"

#include <cloud/blockstore/libs/diagnostics/hostname.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/openssl/crypto/sha.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/stream/format.h>
#include <util/stream/str.h>
#include <util/system/hostname.h>

// TODO: Rewrite this using jinja2 templates or even xslt
// Split this code into markup (html, css, js) and the meaningful part -
// c++ code that generates content (NBS-604)

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

using namespace NActors::NMon;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

using namespace NMonitoringUtils;

////////////////////////////////////////////////////////////////////////////////

TStringBuf AlertClassFromLevel(EAlertLevel alertLevel)
{
    switch (alertLevel)
    {
    case EAlertLevel::SUCCESS:
        return "alert-success";
    case EAlertLevel::INFO:
        return "alert-info";
    case EAlertLevel::WARNING:
        return "alert-warning";
    case EAlertLevel::DANGER:
        return "alert-danger";
    default:
        return {};
    }
}

}   // namespace

namespace NMonitoringUtils {

////////////////////////////////////////////////////////////////////////////////

void BuildNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    const TString& redirect,
    EAlertLevel alertLevel)
{
    out << "<div class='jumbotron";

    auto alertClass = AlertClassFromLevel(alertLevel);
    if (alertClass) {
        out << " " << alertClass;
    }

    out << "'>";
    out << "<h2>";
    out << message;
    out << "</h2>";
    out << "</div>";
    out << "<script type='text/javascript'>\n";
    out << "$(function() {\n";
    out << "    setTimeout(function() {";
    out << "        window.location.href = '";
    out << redirect;
    out << "'";
    out << "    }, 2000);";
    out << "});\n";
    out << "</script>\n";
}

void BuildTabletNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    ui64 tabletId,
    EAlertLevel alertLevel)
{
    BuildNotifyPageWithRedirect(
        out,
        message,
        TStringBuilder() << "../tablets/app?TabletID=" << tabletId,
        alertLevel);
}

void GenerateBlobviewJS(IOutputStream& out)
{
    out << R"___(
        <script type='text/javascript'>
        $(function () {
            $('.view').each(function () {
               $('#actions', this).hide();
               $(this).on('mouseleave', function () { $('#actions', this).hide()});
               $(this).on('mouseenter', function () { $('#actions', this).show()});
            })
        })
        </script>
    )___";
}

void GenerateActionsJS(IOutputStream& out)
{
    out << R"___(
        <script type='text/javascript'>
        function addGarbage() {
            document.forms['AddGarbage'].submit();
        }
        function collectGarbage() {
            document.forms['CollectGarbage'].submit();
        }
        function setBarriers() {
            document.forms['SetHardBarriers'].submit();
        }
        function reassignChannels(hiveId, tabletId) {
            var url = 'app?TabletID=' + hiveId;
            url += '&page=ReassignTablet';
            url += '&tablet=' + tabletId;
            $.ajax({url: url});
        }
        function reassignChannel(hiveId, tabletId, channel) {
            var url = 'app?TabletID=' + hiveId;
            url += '&page=ReassignTablet';
            url += '&tablet=' + tabletId;
            url += '&channel=' + channel;
            $.ajax({url: url});
        }
        function forceCompactionAll() {
            document.forms['ForceCompaction'].submit();
        }
        function forceCompaction(blockIndex) {
            document.forms['ForceCompaction_' + blockIndex].submit();
        }
        function forceCleanupAll() {
            document.forms['ForceCleanup'].submit();
        }
        function rebuildMetadata(rangesPerBatch) {
            document.forms['RebuildMetadata_' + rangesPerBatch].submit();
        }
        function scanDisk(blobsPerBatch) {
            document.forms['ScanDisk_' + blobsPerBatch].submit();
        }
        </script>
    )___";
}

void BuildCreateCheckpointButton(IOutputStream& out, ui64 tabletId)
{
    out << "<form method='POST' name='CreateCheckpoint'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='text' name='checkpointid'/>"
        << "<input type='hidden' name='action' value='createCheckpoint'/>"
        << "<input class='btn btn-primary' type='button' value='Create'"
        << " data-toggle='modal' data-target='#create-checkpoint'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "create-checkpoint",
        "Create checkpoint",
        "Are you sure you want to create a checkpoint?",
        "createCheckpoint();");
}

void BuildDeleteCheckpointButton(
    IOutputStream& out,
    ui64 tabletId,
    const TString& checkpointId)
{
    out << "<center>"
        << "<span class='glyphicon glyphicon-trash' data-toggle='modal' data-target='#delete-checkpoint-"
        << checkpointId << "'>";

    out << "<form method='POST' name='DeleteCheckpoint_" << checkpointId << "' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='checkpointid' value='" << checkpointId << "'/>"
        << "<input type='hidden' name='action' value='deleteCheckpoint'/>"
        << "<input class='btn btn-primary' type='button' value='Delete'"
        << " data-toggle='modal' data-target='#delete-checkpoint-" << checkpointId << "'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "delete-checkpoint-" << checkpointId,
        "Delete checkpoint",
        TStringBuilder()
            << "Are you sure you want to delete checkpoint "
            << checkpointId << "?",
        TStringBuilder()
            << "deleteCheckpoint(\"" << checkpointId << "\");");

    out << "</center>";
}

void BuildMenuButton(IOutputStream& out, const TString& menuItems)
{
    out << "<span class='glyphicon glyphicon-list'"
        << " data-toggle='collapse' data-target='#"
        << menuItems << "' style='padding-right: 5px'>"
        << "</span>";
}

void BuildVolumeTabs(IOutputStream& out)
{
    out << "<ul class='nav nav-tabs' id='Tabs'>"
        << "<li class='active'><a  href='#Overview' data-toggle='tab'>Overview</a></li>"
        << "<li><a href='#History' data-toggle='tab'>History</a></li>"
        << "<li><a href='#Checkpoints' data-toggle='tab'>Checkpoints</a></li>"
        << "<li><a href='#Traces' data-toggle='tab'>Traces</a></li>"
        << "<li><a href='#StorageConfig' data-toggle='tab'>StorageConfig</a></li></ul>";
}

void BuildPartitionTabs(IOutputStream& out)
{
    out << "<ul class='nav nav-tabs' id='Tabs'>"
        << "<li class='active'>" << "<a  href='#Overview' data-toggle='tab'>Overview</a>"
        << "</li>"
        << "<li><a href='#Tables' data-toggle='tab'>Tables</a>" << "</li>"
        << "<li><a href='#Channels' data-toggle='tab'>Channels</a>" << "</li>"
        << "<li><a href='#Index' data-toggle='tab'>Index</a>" << "</li>" << "</ul>";
}

void GeneratePartitionTabsJs(IOutputStream& out)
{
    out << "<script>"
        << "$('#Tabs a').click(function(e) {"
        << "  e.preventDefault();"
        << "  $(this).tab('show');"
        << "});"
        << "$('ul.nav-tabs > li > a').on('shown.bs.tab', function(e) {"
        << "  var id = $(e.target).attr('href').substr(1);"
        << "  window.location.hash = id;"
        << "});"
        << "var hash = window.location.hash;"
        << "$('#Tabs a[href=\"' + hash + '\"]').tab('show');"
        << "</script>";
}

void BuildAddGarbageButton(IOutputStream& out, ui64 tabletId)
{
    out << "<form method='POST' name='AddGarbage'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='text' name='blobs'/>"
        << "<input type='hidden' name='action' value='addGarbage'/>"
        << "<input class='btn btn-primary' type='button' value='Add'"
        << " data-toggle='modal' data-target='#add-garbage'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "add-garbage",
        "Add garbage",
        "Are you sure you want to add garbage?",
        "addGarbage();");
}

void BuildCollectGarbageButton(IOutputStream& out, ui64 tabletId)
{
    out << "<form method='POST' name='CollectGarbage' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='collectGarbage'/>"
        << "<input type='hidden' name='type' value=''/>"
        << "<input class='btn btn-primary' type='button' value='Collect'"
        << " data-toggle='modal' data-target='#collect-garbage'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "collect-garbage",
        "Collect garbage",
        "Are you sure you want to collect garbage?",
        "collectGarbage();");
}

void BuildSetHardBarriers(IOutputStream& out, ui64 tabletId)
{
    out << "<form method='POST' name='SetHardBarriers' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='collectGarbage'/>"
        << "<input type='hidden' name='type' value='hard'/>"
        << "<input class='btn btn-primary' type='button' value='Collect'"
        << " data-toggle='modal' data-target='#set-hard-barriers'/>"
        << "</form>" << Endl;

    BuildConfirmActionDialog(
        out,
        "set-hard-barriers",
        "Set Hard Barriers",
        "Are you sure you want to set hard barriers?",
        "setBarriers();");
}

void BuildReassignChannelsButton(
    IOutputStream& out,
    ui64 hiveTabletId,
    ui64 tabletId)
{
    out << "<a"
        << " href=''"
        << " data-toggle='modal'"
        << " data-target='#reassign-channels'"
        << ">Reassign ALL Channels</a>";

    BuildConfirmActionDialog(
        out,
        "reassign-channels",
        "Reassign ALL channels",
        "Are you sure you want to reassign groups for ALL channels?",
        TStringBuilder()
            << "reassignChannels"
            << "(\"" << hiveTabletId << "\""
            << ",\"" << tabletId << "\""
            << ");");
}

void BuildReassignChannelButton(
    IOutputStream& out,
    ui64 hiveTabletId,
    ui64 tabletId,
    ui32 channel)
{
    out << "<a"
        << " href=''"
        << " data-toggle='modal'"
        << " data-target='#reassign-channel-" << channel << "'"
        << ">Reassign</a>";

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "reassign-channel-" << channel,
        "Reassign channel",
        TStringBuilder()
            << "Are you sure you want to reassign groups for channel "
            << channel << "?",
        TStringBuilder()
            << "reassignChannel"
            << "(\"" << hiveTabletId << "\""
            << ",\"" << tabletId << "\""
            << ",\"" << channel << "\""
            << ");");
}

void BuildForceCompactionButton(IOutputStream& out, ui64 tabletId)
{
    out << "<p><a href='' data-toggle='modal' data-target='#force-compaction'>Force Full Compaction</a></p>"
        << "<form method='POST' name='ForceCompaction' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='compactAll'/>"
        << "<input class='btn btn-primary' type='button' value='Compact ALL ranges'"
        << " data-toggle='modal' data-target='#force-compaction'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        "force-compaction",
        "Force compaction",
        "Are you sure you want to force compaction for ALL ranges?",
        "forceCompactionAll();");
}

void BuildForceCompactionButton(IOutputStream& out, ui64 tabletId, ui32 blockIndex)
{
    out << "<p><a href='' data-toggle='modal' data-target='#force-compaction-"
        << blockIndex
        << "'>Compact</a></p>";

    out << "<form method='POST' name='ForceCompaction_" << blockIndex << "' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='BlockIndex' value='" << blockIndex << "'/>"
        << "<input type='hidden' name='action' value='compact'/>"
        << "<input class='btn btn-primary' type='button' value='Compact'"
        << " data-toggle='modal' data-target='#force-compaction-" << blockIndex << "'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "force-compaction-" << blockIndex,
        "Force compaction",
        TStringBuilder()
            << "Are you sure you want to force compaction for range "
            << blockIndex << "?",
        TStringBuilder() << "forceCompaction(\"" << blockIndex << "\");");
}

void BuildForceCleanupButton(IOutputStream& out, ui64 tabletId)
{
    out << "<p><a href='' data-toggle='modal' data-target='#force-cleanup'>Force Full Cleanup</a></p>"
        << "<form method='POST' name='ForceCleanup' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='cleanupAll'/>"
        << "<input class='btn btn-primary' type='button' value='Cleanup'"
        << " data-toggle='modal' data-target='#force-cleanup'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        "force-cleanup",
        "Force cleanup",
        "Are you sure you want to force cleanup?",
        "forceCleanupAll();");
}

void BuildRebuildMetadataButton(IOutputStream& out, ui64 tabletId, ui32 rangesPerBatch)
{
    out << "<p><a href='' data-toggle='modal' data-target='#rebuild-metadata-"
        << rangesPerBatch
        << "'>Rebuild Metadata in batches of " << rangesPerBatch << " each</a></p>";

    out << "<form method='POST' name='RebuildMetadata_" << rangesPerBatch << "' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='BatchSize' value='" << rangesPerBatch << "'/>"
        << "<input type='hidden' name='action' value='rebuildMetadata'/>"
        << "<input class='btn btn-primary' type='button' value='Rebuild Metadata'"
        << " data-toggle='modal' data-target='#rebuild-metadata-" << rangesPerBatch << "'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "rebuild-metadata-" << rangesPerBatch,
        "Rebuild Metadata",
        TStringBuilder()
            << "Are you sure you want to rebuild partition metadata in batches of "
            << rangesPerBatch << " ranges each?",
        TStringBuilder() << "rebuildMetadata(\"" << rangesPerBatch << "\");");
}

void BuildScanDiskButton(IOutputStream& out, ui64 tabletId, ui32 blobsPerBatch)
{
    out << "<p><a href='' data-toggle='modal' data-target='#scan-disk-"
        << blobsPerBatch
        << "'>Scan Disk in batches of " << blobsPerBatch << " blobs each</a></p>";

    out << "<form method='POST' name='ScanDisk_" << blobsPerBatch << "' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='BatchSize' value='" << blobsPerBatch << "'/>"
        << "<input type='hidden' name='action' value='scanDisk'/>"
        << "<input class='btn btn-primary' type='button' value='Scan Disk'"
        << " data-toggle='modal' data-target='#scan-disk-" << blobsPerBatch << "'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        TStringBuilder() << "scan-disk-" << blobsPerBatch,
        "Scan Disk",
        TStringBuilder()
            << "Are you sure you want to scan disk in batches of "
            << blobsPerBatch << " blobs each?",
        TStringBuilder() << "scanDisk(\"" << blobsPerBatch << "\");");
}

void BuildConfirmActionDialog(
    IOutputStream& out,
    const TString& id,
    const TString& title,
    const TString& message,
    const TString& onClickScript)
{
    out << "<div class='modal fade' id='" << id << "' role='dialog'>";
    out << R"___(
                <div class='modal-dialog'>
                    <div class='modal-content'>
                        <div class='modal-header'>
                            <button type='button' class='close' data-dismiss='modal'>&times;</button>
                            <h4 class='modal-title'>
            )___";

    out << title;

    out << R"___(
                            </h4>
                        </div>
                        <div class='modal-body'>
                            <div class='row'>
                                <div class='col-sm-6 col-md-6'>
            )___";

    out << message;

    out << R"___(
                                </div>
                            </div>
                        </div>
                        <div class='modal-footer'>
                            <button type='submit' class='btn btn-default' data-dismiss='modal' onclick='
            )___";

    out << onClickScript << "'>Confirm</button>";

    out << R"___(
                            <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                        </div>
                    </div>
                </div>
            </div>
            )___";
}

void DumpDefaultHeader(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    ui32 nodeId,
    const TDiagnosticsConfig& config)
{
    HTML(out) {
        TAG(TH3) {
            out << "Tablet <a href='../tablets?TabletID=" << storage.TabletID
                << "'>" << storage.TabletID << "</a>"
                << " running on node <a href='"
                << GetExternalHostUrl(HostName(), EHostService::Nbs, config)
                << "'>" << nodeId << "</a>"
                ;
        }
    }
}

void DumpDescribeHeader(
    IOutputStream& out,
    const TTabletStorageInfo& storage)
{
    static constexpr ui32 startBlockIndex = 0;
    static constexpr ui32 endBlockIndex = 1000;

    HTML(out) {
        TAG(TH3) { out << "DescribeIndex"; }
        DIV() {
            out << "Range: <a href='../tablets/app?TabletID=" << storage.TabletID
                << "&action=describe&range=" << startBlockIndex << ":" << endBlockIndex
                << "'>"<< startBlockIndex << ":" << endBlockIndex << "</a>";
        }
    }
}

void DumpCheckHeader(
    IOutputStream& out,
    const TTabletStorageInfo& storage)
{
    static constexpr ui32 startBlockIndex = 0;
    static constexpr ui32 endBlockIndex = 1000000;

    HTML(out) {
        TAG(TH3) { out << "CheckIndex"; }
        DIV() {
            out << "Range: <a href='../tablets/app?TabletID=" << storage.TabletID
                << "&action=check&range=" << startBlockIndex << ":" << endBlockIndex
                << "'>"<< startBlockIndex << ":" << endBlockIndex << "</a>";
        }
    }
}

void DumpBlockIndex(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    ui32 blockIndex,
    ui64 commitId)
{
    HTML(out) {
        DIV_CLASS("row") {
            DIV_CLASS("col-lg-6") {
                out << "<a href='../tablets/app?TabletID=" << storage.TabletID
                    << "&action=describe&range=" << blockIndex
                    << "'>" << blockIndex << "</a>";
            }
            DIV_CLASS_ID("col-lg-6", "actions") {
                TAG_CLASS_STYLE(TDiv, "col-lg-6", "text-align:right;padding-right:0") {
                    out << "<a href='../tablets/app?TabletID=" << storage.TabletID
                        << "&action=view&block=" << blockIndex
                        << "&commitid=" << commitId
                        << "'>View</a>";
                }
                TAG_CLASS_STYLE(TDiv, "col-lg-6", "text-align:left;padding-right:0") {
                    out << "<a href='../tablets/app?TabletID=" << storage.TabletID
                        << "&action=view&block=" << blockIndex
                        << "&commitid=" << commitId
                        << "&binary=1'>Raw</a>";
                }
            }
        }
    }
}

void DumpBlockIndex(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    ui32 blockIndex)
{
    out << "<a href='../tablets/app?TabletID=" << storage.TabletID
        << "&action=describe&range=" << blockIndex
        << "'>" << blockIndex << "</a>";
}

void DumpBlobId(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    const TPartialBlobId& blobId)
{
    if (blobId) {
        HTML(out) {
            DIV_CLASS("row") {
                auto fullBlobId = MakeBlobId(storage.TabletID, blobId);
                DIV_CLASS("col-lg-9") {
                    out << "<a href='../tablets/app?TabletID=" << storage.TabletID
                        << "&action=describe&blob=" << fullBlobId
                        << "'>" << fullBlobId << "</a>";
                }

                DIV_CLASS_ID("col-lg-3", "actions") {
                    auto groupId = storage.GroupFor(blobId.Channel(), blobId.Generation());
                    TAG_CLASS_STYLE(TDiv, "col-lg-6", "text-align:right;padding-right:0") {
                        out << "<a href='../get_blob?groupId=" << groupId
                            << "&blob=" << fullBlobId << "&debugInfo=1'>View</a>";
                    }
                    TAG_CLASS_STYLE(TDiv, "col-lg-6", "text-align:left;padding-right:0") {
                        out << "<a href='../get_blob?groupId=" << groupId
                            << "&blob=" << fullBlobId << "&binary=1'>Raw</a>";
                    }
                }
            }
        }
    } else {
        out << "-";
    }
}

void DumpBlobOffset(IOutputStream& out, ui16 blobOffset)
{
    if (blobOffset != InvalidBlobOffset) {
        out << blobOffset;
    } else {
        out << "-";
    }
}

void DumpCommitId(IOutputStream& out, ui64 commitId)
{
    if (commitId && commitId != InvalidCommitId) {
        ui64 generation, step;
        std::tie(generation, step) = ParseCommitId(commitId);
        out << commitId << " (Gen: " << generation << ", Step: " << step << ")";
    } else {
        out << "-";
    }
}

void DumpBlobs(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    const TVector<TPartialBlobId>& blobs)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "BlobId"; }
                }
            }
            TABLEBODY() {
                for (const auto& blobId: blobs) {
                    TABLER() {
                        TABLED() { DumpCommitId(out, blobId.CommitId()); }
                        TABLED_CLASS("view") { DumpBlobId(out, storage, blobId); }
                    }
                }
            }
        }
    }
}

void DumpPartitionConfig(
    IOutputStream& out,
    const NProto::TPartitionConfig& config)
{
    auto blockSize = config.GetBlockSize();
    auto blocksCount = config.GetBlocksCount();

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "DiskId"; }
                    TABLED() { out << config.GetDiskId(); }
                }
                TABLER() {
                    TABLED() { out << "BaseDiskId"; }
                    TABLED() { out << config.GetBaseDiskId(); }
                }
                TABLER() {
                    TABLED() { out << "BaseDiskCheckpointId"; }
                    TABLED() { out << config.GetBaseDiskCheckpointId(); }
                }
                TABLER() {
                    TABLED() { out << "ProjectId"; }
                    TABLED() { out << config.GetProjectId(); }
                }
                TABLER() {
                    TABLED() { out << "InstanceId"; }
                    TABLED() { out << config.GetInstanceId(); }
                }
                TABLER() {
                    TABLED() { out << "Block size"; }
                    TABLED() { out << blockSize; }
                }
                TABLER() {
                    TABLED() { out << "Blocks count"; }
                    TABLED() {
                        out << blocksCount << " ("
                            << FormatByteSize(blockSize * blocksCount)
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Channels count"; }
                    TABLED() { out << config.GetChannelsCount(); }
                }
                TABLER() {
                    TABLED() { out << "Storage media kind"; }
                    TABLED() {
                        out << NCloud::NProto::EStorageMediaKind_Name(
                            config.GetStorageMediaKind());
                    }
                }
                TABLER() {
                    TABLED() { out << "Folder Id"; }
                    TABLED() { out << config.GetFolderId(); }
                }
                TABLER() {
                    TABLED() { out << "Cloud Id"; }
                    TABLED() { out << config.GetCloudId(); }
                }
                TABLER() {
                    TABLED() { out << "PerformanceProfile"; }
                    TABLED() {
                        const auto& pp = config.GetPerformanceProfile();
                        TABLE_CLASS("table table-condensed") {
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { out << "MaxReadBandwidth"; }
                                    TABLED() { out << pp.GetMaxReadBandwidth(); }
                                }
                                TABLER() {
                                    TABLED() { out << "MaxWriteBandwidth"; }
                                    TABLED() { out << pp.GetMaxWriteBandwidth(); }
                                }
                                TABLER() {
                                    TABLED() { out << "MaxReadIops"; }
                                    TABLED() { out << pp.GetMaxReadIops(); }
                                }
                                TABLER() {
                                    TABLED() { out << "MaxWriteIops"; }
                                    TABLED() { out << pp.GetMaxWriteIops(); }
                                }
                                TABLER() {
                                    TABLED() { out << "BurstPercentage"; }
                                    TABLED() { out << pp.GetBurstPercentage(); }
                                }
                                TABLER() {
                                    TABLED() { out << "MaxPostponedWeight"; }
                                    TABLED() { out << pp.GetMaxPostponedWeight(); }
                                }
                                TABLER() {
                                    TABLED() { out << "BoostTime"; }
                                    TABLED() { out << pp.GetBoostTime(); }
                                }
                                TABLER() {
                                    TABLED() { out << "BoostRefillTime"; }
                                    TABLED() { out << pp.GetBoostRefillTime(); }
                                }
                                TABLER() {
                                    TABLED() { out << "BoostPercentage"; }
                                    TABLED() { out << pp.GetBoostPercentage(); }
                                }
                                TABLER() {
                                    TABLED() { out << "ThrottlingEnabled"; }
                                    TABLED() { out << pp.GetThrottlingEnabled(); }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void DumpStatsCounters(
    IOutputStream& out,
    const NProto::TIOCounters& counters)
{
    HTML(out) {
        TABLED() { out << "Request count"; }
        TABLED() { out << counters.GetRequestsCount(); }
        TABLED() { out << "Blocks count"; }
        TABLED() { out << counters.GetBlocksCount(); }
        TABLED() { out << "Exec time"; }
        TABLED() { out << counters.GetExecTime(); }
        TABLED() { out << "Wait time"; }
        TABLED() { out << counters.GetWaitTime(); }
    }
}

void DumpPartitionStats(
    IOutputStream& out,
    const NProto::TPartitionConfig& config,
    const NProto::TPartitionStats& stats,
    ui32 freshBlocksCount)
{
    auto blockSize = config.GetBlockSize();

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "Fresh blocks count"; }
                    TABLED() {
                        out << freshBlocksCount << " ("
                            << FormatByteSize(blockSize * freshBlocksCount)
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Mixed blobs count"; }
                    TABLED() { out << stats.GetMixedBlobsCount(); }
                }
                TABLER() {
                    TABLED() { out << "Mixed blocks count"; }
                    TABLED() {
                        out << stats.GetMixedBlocksCount() << " ("
                            << FormatByteSize(blockSize * stats.GetMixedBlocksCount())
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Merged blobs count"; }
                    TABLED() { out << stats.GetMergedBlobsCount(); }
                }
                TABLER() {
                    TABLED() { out << "Merged blocks count"; }
                    TABLED() {
                        out << stats.GetMergedBlocksCount() << " ("
                            << FormatByteSize(blockSize * stats.GetMergedBlocksCount())
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Used blocks count"; }
                    TABLED() {
                        out << stats.GetUsedBlocksCount() << " ("
                            << FormatByteSize(blockSize * stats.GetUsedBlocksCount())
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Logical used blocks count"; }
                    TABLED() {
                        out << stats.GetLogicalUsedBlocksCount() << " ("
                            << FormatByteSize(blockSize * stats.GetLogicalUsedBlocksCount())
                            << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Garbage blocks count"; }
                    TABLED() {
                        out << stats.GetGarbageBlocksCount() << " ("
                            << FormatByteSize(blockSize * stats.GetGarbageBlocksCount())
                            << ")";
                    }
                }
            }
        }
    }
}

void DumpPartitionCounters(
    IOutputStream& out,
    const NProto::TPartitionStats& stats)
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "User read counters"; }
                    DumpStatsCounters(out, stats.GetUserReadCounters());
                }
                TABLER() {
                    TABLED() { out << "User write counters"; }
                    DumpStatsCounters(out, stats.GetUserWriteCounters());
                }
                TABLER() {
                    TABLED() { out << "Sys read counters"; }
                    DumpStatsCounters(out, stats.GetSysReadCounters());
                }
                TABLER() {
                    TABLED() { out << "Sys write counters"; }
                    DumpStatsCounters(out, stats.GetSysWriteCounters());
                }
            }
        }
    }
}

void DumpCompactionMap(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    const TVector<TCompactionCounter>& items,
    const ui32 rangeSize)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "BlockIndex"; }
                    TABLED() { out << "Blobs"; }
                    TABLED() { out << "Blocks"; }
                    TABLED() { out << "UsedBlocks"; }
                    TABLED() { out << "ReadCount"; }
                    TABLED() { out << "BlobsRead"; }
                    TABLED() { out << "BlocksRead"; }
                    TABLED() { out << "Score"; }
                    TABLED() { out << "Compacted"; }
                    TABLED() { out << "Compact"; }
                }
            }
            TABLEBODY() {
                for (const auto& item: items) {
                    TABLER() {
                        TABLED() {
                            out << "<a href='../tablets/app?TabletID=" << storage.TabletID
                                << "&action=describe&range="
                                << item.BlockIndex << ":" << item.BlockIndex + rangeSize
                                << "'>" << item.BlockIndex << "</a>";
                        }
                        TABLED() { out << item.Stat.BlobCount; }
                        TABLED() { out << item.Stat.BlockCount; }
                        TABLED() { out << item.Stat.UsedBlockCount; }
                        TABLED() { out << item.Stat.ReadRequestCount; }
                        TABLED() { out << item.Stat.ReadRequestBlobCount; }
                        TABLED() { out << item.Stat.ReadRequestBlockCount; }
                        TABLED() { out << item.Stat.CompactionScore.Score; }
                        TABLED() { out << item.Stat.Compacted; }
                        TABLED() {
                            BuildForceCompactionButton(
                                out,
                                storage.TabletID,
                                item.BlockIndex);
                        }
                    }
                }
            }
        }
    }
}

void DumpMonitoringVolumeLink(
    IOutputStream& out,
    const TDiagnosticsConfig& config,
    const TString& diskId)
{
    HTML(out) {
        TAG(TH3) {
            out << "<a href='" << GetMonitoringVolumeUrl(config, diskId)
                << "'>Volume dashboards</a>";
        }
    }
}

void DumpMonitoringPartitionLink(
    IOutputStream& out,
    const TDiagnosticsConfig& config)
{
    HTML(out) {
        TAG(TH3) {
            out << "<a href='" << GetMonitoringPartitionUrl(config)
                << "'>Partition dashboards</a>";
        }
    }
}

void DumpBlockContent(IOutputStream& out, const TString& data)
{
    const size_t rowSize = 32;
    for (size_t offset = 0; offset < data.size(); offset += rowSize) {
        out << Sprintf("0x%06zx | ", offset);
        size_t i = 0;
        for (; i < rowSize && i + offset < data.size(); ++i) {
            out << Sprintf("%02x ", (ui8)data[i + offset]);
        }
        for (; i < rowSize; ++i) {
            out << "   ";
        }
        out << "| ";
        for (i = 0; i < rowSize && i + offset < data.size(); ++i) {
            ui8 ch = data[offset + i];
            if (isprint(ch)) {
                out << ch;
            } else {
                out << ".";
            }
        }
        out << "\n";
    }
}

void DumpDataHash(IOutputStream& out, const TString& data)
{
    bool haveNonZeroes = false;
    for (const auto c: data) {
        if (c) {
            haveNonZeroes = true;
            break;
        }
    }

    if (haveNonZeroes) {
        auto digest = NOpenSsl::NSha1::Calc(data.data(), data.size());
        for (ui32 i = 0; i < NOpenSsl::NSha1::DIGEST_LENGTH; ++i) {
            out << Hex(digest[i]);
            if (i < NOpenSsl::NSha1::DIGEST_LENGTH - 1) {
                out << ' ';
            }
        }
    } else {
        out << "EMPTY";
    }
}

void DumpTabletNotReady(IOutputStream& out)
{
    HTML(out) {
        TAG(TH3) {
            out << "Tablet not ready yet";
        }
    }
}

TCgiParameters GatherHttpParameters(const TEvRemoteHttpInfo& msg)
{
    auto params = msg.Cgi();
    if (const auto& ext = msg.ExtendedQuery;
        ext && ext->GetMethod() == HTTP_METHOD_POST)
    {
        for (const auto& param : ext->GetPostParams()) {
            params.emplace(param.GetKey(), param.GetValue());
        }
    }
    return params;
}

TCgiParameters GetHttpMethodParameters(const TEvRemoteHttpInfo& msg)
{
    if (msg.GetMethod() != HTTP_METHOD_POST) {
        return msg.Cgi();
    }

    TCgiParameters params;
    if (const auto& ext = msg.ExtendedQuery;
        ext && ext->GetMethod() == HTTP_METHOD_POST)
    {
        for (const auto& param : ext->GetPostParams()) {
            params.emplace(param.GetKey(), param.GetValue());
        }
    }

   return params;
}

HTTP_METHOD GetHttpMethodType(const NActors::NMon::TEvRemoteHttpInfo& msg)
{
    if (const auto& ext = msg.ExtendedQuery; ext)
    {
        return static_cast<HTTP_METHOD>(ext->GetMethod());
    }
    return msg.GetMethod();
}

}   // namespace NMonitoringUtils
}   // namespace NCloud::NBlockStore::NStorage
