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

<xsl:template match="test-case">
    <xsl:param name="date"/>
    <div>Test Case <xsl:value-of select="@name"/></div>
    <div style="font-weight: bold">
        <xsl:value-of select="$date"/> run:
    </div>
    <div style="border-bottom: 1px dashed black; padding: 10px;">
        <div>
            <xsl:attribute name="style">
                <xsl:choose>
                    <xsl:when test="@error">background-color: mistyrose</xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="@highlight_color">background-color: <xsl:value-of select="@highlight_color"/></xsl:when>
                            <xsl:otherwise>background-color: #c0f08d</xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <div>
                <div style="display: inline-block; width: 50%">
                    <xsl:apply-templates select="params"/>
                </div>
                <div style="display: inline-block; width: 50%; float: right; font-weight: bold">
                    <xsl:choose>
                        <xsl:when test="@error">Error: <xsl:value-of select="@error"/></xsl:when>
                        <xsl:otherwise>OK</xsl:otherwise>
                    </xsl:choose>
                </div>
            </div>
        </div>
    </div>
</xsl:template>

<xsl:template match="suite">
    <xsl:call-template name="logs" select="."/>
    <div class="report" width="100%">
        <xsl:apply-templates select="test-case">
            <xsl:with-param name="date">
                <xsl:value-of select="@date"/>
            </xsl:with-param>
        </xsl:apply-templates>
    </div>
</xsl:template>

<xsl:template match="report">
    <html>
        <xsl:call-template name="head"/>
        <body>
            <xsl:if test="@name">
                <h2><xsl:value-of select="@name"/></h2>
            </xsl:if>
        <xsl:apply-templates select="suite"/>
        </body>
    </html>
</xsl:template>

</xsl:stylesheet>
