<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template name="head">
    <link rel="stylesheet" href="/style/generic.css"/>
</xsl:template>

<xsl:template match="artifact">
    <div>
        <a>
            <xsl:attribute name="href">
                <xsl:value-of select="@link"/>
            </xsl:attribute>
            <xsl:value-of select="@name"/>
        </a>
    </div>
</xsl:template>

<xsl:template name="logs">
    <xsl:if test="@stderr and @stdout">
        <div style="border-bottom: 1px dashed black; padding: 10px;">
            <span style="font-weight: bold;">
                <a>
                    <xsl:attribute name="href">
                        <xsl:value-of select="@stderr"/>
                    </xsl:attribute>
                    stderr
                </a>
            </span>
            <span style="margin-left: 10px; font-weight: bold;">
                <a>
                    <xsl:attribute name="href">
                        <xsl:value-of select="@stdout"/>
                    </xsl:attribute>
                    stdout
                </a>
            </span>
        </div>
    </xsl:if>
</xsl:template>

</xsl:stylesheet>
