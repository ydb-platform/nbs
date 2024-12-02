R"(<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<xsl:template match="root">
    <h3>Tablet <a>
        <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
        <xsl:value-of select="tablet_id"/>
        </a> running on node <xsl:value-of select="header_hostname"/><xsl:text>[</xsl:text><xsl:value-of select="node_id"/><xsl:text>]</xsl:text>
    </h3>
    <h3>Info</h3>
    <div>Filesystem Id: <xsl:value-of select="fs_id"/></div>
    <div>Block size: <xsl:value-of select="block_size"/></div>
    <div>Blocks: <xsl:value-of select="blocks_count"/></div>
    <div>Tablet host: <xsl:value-of select="tablet_host"/></div>
    <xsl:if test="shards">
        <h3>Shards</h3>
        <table class="table table-bordered">
            <thead>
                <tr>
                    <th>ShardNo</th>
                    <th>FileSystemId</th>
                </tr>
            </thead>
            <xsl:for-each select="shards/cd">
                <tr>
                    <td><xsl:value-of select="shard_no"/></td>
                    <td>
                        <a>
                            <xsl:attribute name="href">../filestore/service?action=search&amp;Filesystem=<xsl:value-of select="shard_id"/></xsl:attribute>
                            <xsl:value-of select="shard_id"/>
                        </a>
                    </td>
                </tr>
            </xsl:for-each>
        </table>
    </xsl:if>
    <h3>State</h3>
    <div>Current commitId: <xsl:value-of select="curr_commit_id"/></div>
    <xsl:for-each select="ops_state/cd">
        <div>
            <xsl:value-of select="name"/> state: <xsl:value-of select="state"/>, Timestamp : <xsl:value-of select="timestamp"/>, Completed: <xsl:value-of select="completed"/>, Failed: <xsl:value-of select="failed"/>, Backoff: <xsl:value-of select="backoff"/>
        </div>
    </xsl:for-each>
    <h3>Stats</h3>
    <pre><xsl:value-of select="stats"/></pre>
    <h3>CompactionMap</h3>
    <table class="table table-bordered">
        <thead>
            <tr>
                <th>Used ranges count</th>
                <th>Allocated ranges count</th>
                <th>Compaction map density</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><xsl:value-of select="used_ranges_count"/></td>
                <td><xsl:value-of select="allocated_ranges_count"/></td>
                <td>
                    <div class="progress">
                        <div class='progress-bar' role='progressbar' aria-valuemin='0'>
                            <xsl:attribute name="style">width: <xsl:value-of select="ranges_percents"/>%</xsl:attribute>
                            <xsl:attribute name="aria-valuenow"><xsl:value-of select="used_ranges_count"/></xsl:attribute>
                            <xsl:attribute name="aria-valuemax"><xsl:value-of select="allocated_ranges_count"/></xsl:attribute>
                            <xsl:value-of select="ranges_percents"/>%
                        </div>
                    </div>
                </td>
            </tr>
        </tbody>
    </table>
    <xsl:for-each select="compaction_range_info/cd">
        <h4><xsl:value-of select="name"/></h4>
        <table class="table table-bordered table-sortable">
            <thead>
                <tr>
                    <th>Range</th>
                    <th>Blobs</th>
                    <th>Deletions</th>
                </tr>
            </thead>
            <xsl:for-each select="ranges/cd">
                <tr>
                    <td>
                        <div class="col-lg-9">
                            <a>
                                <xsl:attribute name="href">../tablets/app?TabletID=<xsl:value-of select="../../../../tablet_id"/>&amp;action=dumpRange&amp;rangeId=<xsl:value-of select="id"/></xsl:attribute>
                                <xsl:value-of select="id"/>
                            </a>
                        </div>
                    </td>
                    <td><xsl:value-of select="blobs"/></td>
                    <td><xsl:value-of select="deletions"/></td>
                </tr>
            </xsl:for-each>
        </table>
    </xsl:for-each>
    
    <h3>Backpressure</h3>
    <xsl:choose>
    <xsl:when test="write">
        <div class="alert">Write allowed</div>
    </xsl:when>
    <xsl:otherwise>
        <div class="alert alert-danger">Write NOT allowed: <xsl:value-of select="write_msg"/></div>
    </xsl:otherwise>
    </xsl:choose>
    <xsl:if test="backpressure_error_count &gt; 0 or backpressure_period_start &gt; 0">
        <div class="alert">Backpressure errors: <xsl:value-of select="backpressure_error_count"/></div>
        <div class="alert">Backpressure period start: <xsl:value-of select="backpressure_period_start"/></div>
        <div class="alert">Backpressure period: <xsl:value-of select="backpressure_period"/></div>
    </xsl:if>
    <table class="table table-bordered">
        <thead>
            <tr>
                <th>Name</th>
                <th>Value</th>
                <th>Threshold</th>
            </tr>
        </thead>
        <xsl:for-each select="backpressure/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
                <td><xsl:value-of select="threshold"/></td>
            </tr>
        </xsl:for-each>
    </table>
    <h3>CompactionInfo</h3>
    <table class="table table-bordered">
        <thead>
            <tr>
                <th>Parameter</th>
                <th>Value</th>
            </tr>
        </thead>
        <xsl:for-each select="compaction/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
            </tr>
        </xsl:for-each>
    </table>
    <h3>CleanupInfo</h3>
    <table class="table table-bordered">
        <thead>
            <tr>
                <th>Parameter</th>
                <th>Value</th>
            </tr>
        </thead>
        <xsl:for-each select="cleanup/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
            </tr>
        </xsl:for-each>
    </table>
    <xsl:choose>
        <xsl:when test="forced_op">
            <h3>
                CompactionQueue
            </h3>
            <div class="progress">
                <div class='progress-bar' role='progressbar' aria-valuemin='0'>
                    <xsl:attribute name="style">width: <xsl:value-of select="compact_percents"/>%</xsl:attribute>
                    <xsl:attribute name="aria-valuenow"><xsl:value-of select="curr_compact"/></xsl:attribute>
                    <xsl:attribute name="aria-valuemax"><xsl:value-of select="max_compact"/></xsl:attribute>
                    <xsl:value-of select="compact_percents"/>%
                </div>
            </div>
            <xsl:value-of select="curr_compact"/> of <xsl:value-of select="max_compact"/>
        </xsl:when>
        <xsl:otherwise>
            <h3>
                <span class='glyphicon glyphicon-list' data-toggle='collapse' data-target='#compact-all' style='padding-right: 5px'></span>
                CompactionQueue
            </h3>
            <div class='collapse form-group' id='compact-all'>
                <p><a href='' data-toggle='modal' data-target='#force-compaction'>Force Full Compaction</a></p>
                <form method='POST' name='ForceCompaction' style='display:none'>
                    <input type='hidden' name='TabletID'>
                        <xsl:attribute name="value"><xsl:value-of select="tablet_id"/></xsl:attribute>
                    </input>
                    <input type='hidden' name='action' value='forceOperationAll'/>
                    <input type='hidden' name='mode' value='compaction'/>
                    <input class='btn btn-primary' type='button' value='Compact ALL ranges' data-toggle='modal' data-target='#force-compaction'/>
                </form>
                <div class='modal fade' id='force-compaction' role='dialog'>
                    <div class='modal-dialog'>
                        <div class='modal-content'>
                            <div class='modal-header'>
                                <button type='button' class='close' data-dismiss='modal'>&amp;times;</button>
                                <h4 class='modal-title'>
                                    Force compaction
                                </h4>
                            </div>
                            <div class='modal-body'>
                                <div class='row'>
                                    <div class='col-sm-6 col-md-6'>
                                        Are you sure you want to force compaction for ALL ranges?
                                    </div>
                                </div>
                            </div>
                            <div class='modal-footer'>
                                <button type='submit' class='btn btn-default' data-dismiss='modal' onclick='forceCompactionAll();'>Confirm</button>
                                <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                            </div>
                        </div>
                    </div>
                </div>
                <p><a href='' data-toggle='modal' data-target='#force-cleanup'>Force Full Cleanup</a></p>
                <form method='POST' name='ForceCleanup' style='display:none'>
                    <input type='hidden' name='TabletID'>
                        <xsl:attribute name="value"><xsl:value-of select="tablet_id"/></xsl:attribute>
                    </input>
                    <input type='hidden' name='action' value='forceOperationAll'/>
                    <input type='hidden' name='mode' value='cleanup'/>
                    <input class='btn btn-primary' type='button' value='Cleanup ALL ranges' data-toggle='modal' data-target='#force-cleanup'/>
                </form>
                <div class='modal fade' id='force-cleanup' role='dialog'>
                    <div class='modal-dialog'>
                        <div class='modal-content'>
                            <div class='modal-header'>
                                <button type='button' class='close' data-dismiss='modal'>&amp;times;</button>
                                <h4 class='modal-title'>
                                    Force cleanup
                                </h4>
                            </div>
                            <div class='modal-body'>
                                <div class='row'>
                                    <div class='col-sm-6 col-md-6'>
                                        Are you sure you want to force cleanup for ALL ranges?
                                    </div>
                                </div>
                            </div>
                            <div class='modal-footer'>
                                <button type='submit' class='btn btn-default' data-dismiss='modal' onclick='forceCleanupAll();'>Confirm</button>
                                <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                            </div>
                        </div>
                    </div>
                </div>
                <p><a href='' data-toggle='modal' data-target='#force-delete-zero-compaction-ranges'>Force Delete zero compaction ranges</a></p>
                <form method='POST' name='ForceDeleteZeroCompactionRanges' style='display:none'>
                    <input type='hidden' name='TabletID'>
                        <xsl:attribute name="value"><xsl:value-of select="tablet_id"/></xsl:attribute>
                    </input>
                    <input type='hidden' name='action' value='forceOperationAll'/>
                    <input type='hidden' name='mode' value='deleteZeroCompactionRanges'/>
                    <input class='btn btn-primary' type='button' value='Delete zero compaction ranges' data-toggle='modal' data-target='#force-delete-zero-compaction-ranges'/>
                </form>
                <div class='modal fade' id='force-delete-zero-compaction-ranges' role='dialog'>
                    <div class='modal-dialog'>
                        <div class='modal-content'>
                            <div class='modal-header'>
                                <button type='button' class='close' data-dismiss='modal'>&amp;times;</button>
                                <h4 class='modal-title'>
                                    Force delete zero compaction ranges
                                </h4>
                            </div>
                            <div class='modal-body'>
                                <div class='row'>
                                    <div class='col-sm-6 col-md-6'>
                                        Are you sure you want to delete ALL zero compaction ranges?
                                    </div>
                                </div>
                            </div>
                            <div class='modal-footer'>
                                <button type='submit' class='btn btn-default' data-dismiss='modal' onclick='forceDeleteZeroCompactionRanges();'>Confirm</button>
                                <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </xsl:otherwise>
    </xsl:choose>
    <h3>Channels</h3>
    <div>
        <p>
            <a>
                <xsl:attribute name="href">app?TabletID=<xsl:value-of select="hive_tablet_id"/>&amp;page=Groups&amp;tablet_id=<xsl:value-of select="storage_tablet_id"/></xsl:attribute>
                Channel history
            </a>
        </p>
        <table class="table table-condensed">
            <tbody>
                <xsl:for-each select="channels/cd">
                    <tr>
                        <td>Channel: <xsl:value-of select="channel"/></td>
                        <td>StoragePool: <xsl:value-of select="storage_pool"/></td>
                        <td>Id: <xsl:value-of select="group_id"/></td>
                        <td>Gen: <xsl:value-of select="generation"/></td>
                        <td>PoolKind: <xsl:value-of select="pool_kind"/></td>
                        <td>DataKind: <xsl:value-of select="data_kind"/></td>
                        <td>
                            <xsl:choose>
                                <xsl:when test="system_writable">
                                    <xsl:choose>
                                        <xsl:when test="writable">
                                            <span class="label" style="background-color: green">Writable<xsl:if test="free != 0"> free=<xsl:value-of select="free"/>%</xsl:if></span>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <span class="label" style="background-color: yellow">SystemWritable<xsl:if test="free != 0"> free=<xsl:value-of select="free"/>%</xsl:if></span>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:choose>
                                        <xsl:when test="writable">
                                            <span class="label" style="background-color: pink">WeirdState<xsl:if test="free != 0"> free=<xsl:value-of select="free"/>%</xsl:if></span>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <span class="label" style="background-color: orange">Readonly<xsl:if test="free != 0"> free=<xsl:value-of select="free"/>%</xsl:if></span>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:otherwise>
                            </xsl:choose>
                        </td>
                        <td>
                            <a>
                                <xsl:attribute name="href">../actors/blobstorageproxies/blobstorageproxy<xsl:value-of select="group_id"/></xsl:attribute>
                                Status
                            </a>
                        </td>
                        <xsl:if test="group_url">
                            <td>
                                    
                                <a>
                                    <xsl:attribute name="href">.<xsl:value-of select="group_url"/></xsl:attribute>
                                    Graphs
                                </a>
                            </td>
                        </xsl:if>
                    </tr>
                </xsl:for-each>
            </tbody>
        </table>
    </div>
    <h3>Profiling allocator stats</h3>
    <table class="table table-condensed">
        <tbody>
        <xsl:for-each select="alloc_stats/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
            </tr>
        </xsl:for-each>
        </tbody>
    </table>
    <h3>Blob index stats</h3>
    <h3>Performance profile</h3>
    <table class="table table-condensed">
        <tbody>
        <xsl:for-each select="profile_stats/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
            </tr>
        </xsl:for-each>
        </tbody>
    </table>
    <xsl:if test="used_profile_stats">
        <h3>Used performance profile</h3>
        <table class="table table-condensed">
            <tbody>
            <xsl:for-each select="used_profile_stats/cd">
                <tr>
                    <td><xsl:value-of select="name"/></td>
                    <td><xsl:value-of select="value"/></td>
                </tr>
            </xsl:for-each>
            </tbody>
        </table>
    </xsl:if>
    <h3>Throttler state</h3>
    <table class="table table-condensed">
        <tbody>
        <xsl:for-each select="throttler_state/cd">
            <tr>
                <td><xsl:value-of select="name"/></td>
                <td><xsl:value-of select="value"/></td>
            </tr>
        </xsl:for-each>
        <tr>
            <td>ThrottlerParams</td>
            <td>
                <table class="table table-condensed">
                    <tbody>
                        <xsl:for-each select="throttler_params/cd">
                            <tr>
                                <td><xsl:value-of select="name"/></td>
                                <td><xsl:value-of select="value"/></td>
                            </tr>
                        </xsl:for-each>
                    </tbody>
                </table>
            </td>
        </tr>
        </tbody>
    </table>
    <xsl:if test="storage_config">
        <h3>StorageConfig overrides</h3>
        <table class="table table-bordered table-sortable">
            <xsl:for-each select="storage_config/config_properties/cd">
                <tr>
                    <td><xsl:value-of select="name"/></td>
                    <td><xsl:value-of select="value"/></td>
                </tr>
            </xsl:for-each>
        </table>
    </xsl:if>
    <h3>Active Sessions</h3>
    <table class="table table-bordered table-sortable">
        <thead>
            <tr>
                <th>ClientId</th>
                <th>FQDN</th>
                <th>SessionId</th>
                <th>Recovery</th>
                <th>SeqNo</th>
                <th>ReadOnly</th>
                <th>Owner</th>
            </tr>
        </thead>
        <xsl:for-each select="sessions/cd">
            <tr>
                <td><xsl:value-of select="client_id"/></td>
                <td><xsl:value-of select="fqdn"/></td>
                <td><xsl:value-of select="session_id"/></td>
                <td><xsl:value-of select="recovery"/></td>
                <td><xsl:value-of select="seq_no"/></td>
                <td><xsl:value-of select="readonly"/></td>
                <td><xsl:value-of select="owner"/></td>
            </tr>
        </xsl:for-each>
    </table>
    <h3>Session history</h3>
    <table class="table table-bordered table-sortable">
        <thead>
            <tr>
                <th>ClientId</th>
                <th>FQDN</th>
                <th>Timestamp</th>
                <th>SessionId</th>
                <th>ActionType</th>
            </tr>
        </thead>
        <xsl:for-each select="session_history/cd">
            <tr>
                <td><xsl:value-of select="client_id"/></td>
                <td><xsl:value-of select="fqdn"/></td>
                <td><xsl:value-of select="timestamp"/></td>
                <td><xsl:value-of select="session_id"/></td>
                <td><xsl:value-of select="action_type"/></td>
            </tr>
        </xsl:for-each>
    </table>
    <script type='text/javascript'>
        function forceCompactionAll() {
            document.forms['ForceCompaction'].submit();
        }
    </script>
    <script type='text/javascript'>
        function forceCleanupAll() {
            document.forms['ForceCleanup'].submit();
        }
    </script>
    <script type='text/javascript'>
        function forceDeleteZeroCompactionRanges() {
            document.forms['ForceDeleteZeroCompactionRanges'].submit();
        }
    </script>
</xsl:template>
</xsl:stylesheet>
)"