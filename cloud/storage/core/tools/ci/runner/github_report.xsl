<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:include href="common.xsl"/>

<xsl:template match="suite">
    <tr>
        <td><xsl:value-of select="@name"/></td>
        <td>
            <xsl:attribute name="style">
                color: <xsl:value-of select="@status-color"/>
            </xsl:attribute>
            <xsl:value-of select="@status-message"/>
        </td>
        <td>
            <xsl:apply-templates select="artifact"/>
        </td>
    </tr>
</xsl:template>

<xsl:template match="report">
    <html>
        <xsl:call-template name="head"/>
        <body>
            <h2><xsl:value-of select="@name"/></h2>
            <table class="report">
                <tr>
                    <th>Suite</th>
                    <th>Status</th>
                    <th>Artifacts</th>
                </tr>
                <xsl:apply-templates select="suite"/>
            </table>
        </body>
    </html>
</xsl:template>

</xsl:stylesheet>
