R"(<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<xsl:template match="root">
    <xsl:choose>
    <xsl:when test="has_data">
        <h3>Search Filesystem by id</h3>
        <form method="GET" id="fsSearch" name="fsSearch">
            Filesystem: <input type="text" id="Filesystem" name="Filesystem"/>
            <input class="btn btn-primary" type="submit" value="Search"/> 
            <input type='hidden' name='action' value='search'/>
        </form>
        <h3>Local Sessions</h3>
        <table class="table table-bordered table-sortable">
            <thead>
                <tr>
                    <th>ClientId</th>
                    <th>FileSystemId</th>
                    <th>SessionId</th>
                </tr>
            </thead>
            <xsl:for-each select="sessions/cd">
                <tr>
                    <td><xsl:value-of select="client_id"/></td>
                    <td>
                        <a>
                            <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
                            <xsl:value-of select="fs_id"/>
                        </a>
                    </td>
                    <td><xsl:value-of select="session_id"/></td>
                </tr>
            </xsl:for-each>
        </table>
        <h3>Local Filesystems</h3>
        <table class="table table-bordered table-sortable">
            <thead>
                <tr>
                    <th>FileStore</th>
                    <th>Tablet</th>
                    <th>Size</th>
                    <th>Media kind</th>
                </tr>
            </thead>
            <xsl:for-each select="local_filesystems/cd">
                <tr>
                    <td>
                        <a>
                            <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
                            <xsl:value-of select="fs_id"/>
                        </a>
                    </td>
                    <td>
                        <a>
                            <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
                            <xsl:value-of select="tablet_id"/>
                        </a>
                    </td>
                    <td><xsl:value-of select="size"/></td>
                    <td><xsl:value-of select="media_kind"/></td>
                </tr>
            </xsl:for-each>
        </table>
        <h3>Config</h3>
        <table class="table table-condensed">
            <tbody>
                <xsl:for-each select="config_table/config_propertiries/cd">
                    <tr>
                        <td><xsl:value-of select="name"/></td>
                        <td><xsl:value-of select="value"/></td>
                    </tr>
                </xsl:for-each>
            </tbody>
        </table>
    </xsl:when>
    <xsl:otherwise>
        State not ready yet
    </xsl:otherwise>
    </xsl:choose>
</xsl:template>
</xsl:stylesheet>
)"