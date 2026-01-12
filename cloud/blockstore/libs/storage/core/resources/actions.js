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
    $.ajax({ url: url });
}
function reassignChannel(hiveId, tabletId, channel) {
    var url = 'app?TabletID=' + hiveId;
    url += '&page=ReassignTablet';
    url += '&tablet=' + tabletId;
    url += '&channel=' + channel;
    $.ajax({ url: url });
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
