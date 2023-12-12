<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:include href="common.xsl"/>

<xsl:template match="/ | @* | node()">
    <xsl:copy>
        <xsl:apply-templates select="@* | node()" />
    </xsl:copy>
</xsl:template>

<xsl:template match="params">
    <code>
        <pre>
            <xsl:value-of select="text()"/>
        </pre>
    </code>
</xsl:template>

<xsl:template match="test-case-report">
    <tr>
        <td>
            <table>
                <tr>
                    <td width="20%">
                        <xsl:apply-templates select="params"/>
                    </td>
                    <td width="80%">
                        <xsl:apply-templates select="svg"/>
                    </td>
                </tr>
            </table>
        </td>
    </tr>
</xsl:template>

<xsl:template match="test-case">
    <xsl:param name="date"/>
    <tr>
        <th style="border-top: 1px dashed black; padding: 10px;" colspan="2">Test Case <xsl:value-of select="@name"/></th>
    </tr>
    <tr>
        <td colspan="2">
            <b><xsl:value-of select="$date"/> run:</b>
        </td>
    </tr>
    <xsl:apply-templates select="test-case-report"/>
    <tr>
        <td colspan="2">
            <b>History:</b>
        </td>
    </tr>
    <tr>
        <td colspan="2">
            <a>
                <xsl:attribute name="href">
                    <xsl:value-of select="@history-link"/>
                </xsl:attribute>
                <img>
                    <xsl:attribute name="src">
                        <xsl:value-of select="@history-thumb"/>
                    </xsl:attribute>
                </img>
            </a>
        </td>
    </tr>
</xsl:template>

<xsl:template match="suite">
    <xsl:call-template name="logs" select="."/>
    <table class="report" width="100%">
        <xsl:apply-templates select="test-case">
            <xsl:with-param name="date">
                <xsl:value-of select="@date"/>
            </xsl:with-param>
        </xsl:apply-templates>
    </table>
</xsl:template>

<xsl:template match="report">
    <html>
        <xsl:call-template name="head"/>
        <body>
            <h2><xsl:value-of select="@name"/></h2>
        <xsl:apply-templates select="suite"/>
        </body>
    </html>
</xsl:template>

</xsl:stylesheet>
