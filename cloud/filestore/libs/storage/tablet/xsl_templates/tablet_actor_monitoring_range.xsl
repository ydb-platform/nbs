<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<xsl:template match="root">
    <h3>Tablet <a>
        <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
        <xsl:value-of select="tablet_id"/>
        </a> running on node <xsl:value-of select="hostname"/><xsl:text>[</xsl:text><xsl:value-of select="node_id"/><xsl:text>]</xsl:text>
    </h3>
    <h3>RangeId: <xsl:value-of select="range_id"/></h3>
    <table class="table table-bordered table-sortable">
        <thead>
            <tr>
                <th># NodeId</th>
                <th>BlockIndex</th>
                <th>BlobId</th>
                <th>MinCommitId</th>
                <th>MaxCommitId</th>
            </tr>
        </thead>
        <tbody>
            <xsl:for-each select="blocks/cd">
                <tr>
                    <td><xsl:value-of select="node_id"/></td>
                    <td><xsl:value-of select="block_index"/></td>
                    <td><xsl:value-of select="blob_id"/></td>
                    <td><xsl:value-of select="min_commit_id"/></td>
                    <td><xsl:value-of select="max_commit_id"/></td>
                </tr>
            </xsl:for-each>
        </tbody>
    </table>
</xsl:template>
</xsl:stylesheet>
