<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<xsl:template match="root">
    <h3>FileSystem</h3>
    <table class="table table-bordered">
        <thead>
            <tr>
                <th>FileSystem</th>
                <th>Tablet ID</th>
            </tr>
        </thead>
        <tr>
            <td><xsl:value-of select="path"/></td>
            <td>
                <a>
                    <xsl:attribute name="href">../tablets?TabletID=<xsl:value-of select="tablet_id"/></xsl:attribute>
                    <xsl:value-of select="tablet_id"/>
                </a>
            </td>
        </tr>
    </table>
</xsl:template>
</xsl:stylesheet>
