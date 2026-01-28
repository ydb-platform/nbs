<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:include href="common.xsl"/>

<xsl:template match="report-info">
    <tr>
        <td>
            <div>
                <a>
                    <xsl:attribute name="href">
                        <xsl:value-of select="@link"/>
                    </xsl:attribute>
                    <xsl:value-of select="@name"/>
                </a>
            </div>
        </td>
        <td>
            <div>
                <a>
                    <xsl:attribute name="href">
                        <xsl:value-of select="@junit-link"/>
                    </xsl:attribute>
                    <xsl:attribute name="style">
                        color: <xsl:value-of select="@status-color"/>
                    </xsl:attribute>
                    <xsl:value-of select="@status-message"/>
                </a>
            </div>
        </td>
    </tr>
</xsl:template>

<xsl:template match="status">
    <html>
        <xsl:call-template name="head"/>
        <body>
            <h2><xsl:value-of select="@name"/></h2>
            <table class="status">
                <tr>
                    <th>Report</th>
                    <th>Status</th>
                </tr>
                <xsl:apply-templates select="report-info"/>
            </table>
        </body>
    </html>
</xsl:template>

</xsl:stylesheet>
