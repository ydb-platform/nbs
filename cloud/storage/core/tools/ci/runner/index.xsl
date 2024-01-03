<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:include href="common.xsl"/>

<!-- elements -->

<xsl:template match="report-info">
    <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
        <div>
            Last Report:
            <span style="font-weight: bold;"><xsl:value-of select="@name"/></span>
        </div>
        <div>
            <a>
                <xsl:attribute name="href">
                    <xsl:value-of select="@link"/>
                </xsl:attribute>
                Last Report Link
            </a>
        </div>
        <div>
            <xsl:attribute name="style">
                color: <xsl:value-of select="@status-color"/>
            </xsl:attribute>
            Last Report Status: <xsl:value-of select="@status-message"/>
        </div>
    </div>
</xsl:template>

<xsl:template match="tests">
    <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
        <h3>uts/pytests (last <xsl:value-of select="count(report-info)"/> runs)</h3>
        <xsl:apply-templates select="report-info"/>
        <div>
            <a href="/ci/tests_index.html">All Reports</a>
        </div>
    </div>
</xsl:template>

<xsl:template match="test-case-status">
    <div>
        <span>Test Case: <xsl:value-of select="@name"/></span>
        <span>
            <xsl:attribute name="style">
                color: <xsl:value-of select="@status-color"/>
            </xsl:attribute>
            Status: <xsl:value-of select="@status-message"/>
        </span>
    </div>
</xsl:template>

<xsl:template match="suite-status">
    <xsl:param name="show-all-testsuites" select="true()"/>

    <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
        <div>Suite: <xsl:value-of select="@type"/></div>
        <div>
            Run:
            <span style="font-weight: bold;"><xsl:value-of select="@name"/></span>
        </div>
        <div>
            <a>
                <xsl:attribute name="href">
                    <xsl:value-of select="@link"/>
                </xsl:attribute>
                Run Link
            </a>
        </div>
        <div>
            <xsl:attribute name="style">
                color: <xsl:value-of select="@status-color"/>
            </xsl:attribute>
            Run Status: <xsl:value-of select="@status-message"/>
        </div>
        <xsl:if test="$show-all-testsuites">
            <div style="border: 1px solid black; margin-bottom: 10px; padding: 5px;">
                <xsl:apply-templates select="test-case-status"/>
            </div>
        </xsl:if>
    </div>
</xsl:template>

<!-- detailed suite report -->

<xsl:template name="detailed">
    <xsl:param name="suite-kind"/>
    <xsl:param name="show-all-testsuites" select="true()"/>
    <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
        <h3><xsl:value-of select="$suite-kind"/> tests (last <xsl:value-of select="count(suite-status)"/> runs)</h3>
        <xsl:apply-templates select="suite-status">
            <xsl:with-param name="show-all-testsuites" select="$show-all-testsuites"/>
        </xsl:apply-templates>
        <div>
            <a href="/ci/results/{$suite-kind}.html">All Runs</a>
        </div>
    </div>
</xsl:template>

<!-- brief suite report -->

<xsl:template name="brief">
    <xsl:param name="suite-kind"/>
    <div style="border-top: 1px dashed black; margin-bottom: 10px; padding-top: 10px">
        <xsl:for-each select="suite-status[not(@type = preceding-sibling::*[1]/@type)]">
            <div>
                <xsl:value-of select="$suite-kind"/> test suite<xsl:text> </xsl:text>
                <span style="font-weight: bold;"><xsl:value-of select="@type"/><xsl:text> </xsl:text></span>
                run <span style="font-weight: bold;"><xsl:value-of select="@name"/><xsl:text> </xsl:text></span>
                <span>
                    <xsl:attribute name="style">
                        color: <xsl:value-of select="@status-color"/><xsl:text> </xsl:text>
                    </xsl:attribute>
                    <xsl:value-of select="@status-message"/><xsl:text> </xsl:text>
                </span>
                <a>
                    <xsl:attribute name="href">
                        <xsl:value-of select="@link"/>
                    </xsl:attribute>
                    Run Link
                </a>
            </div>
        </xsl:for-each>
    </div>
</xsl:template>

<!-- boilerplate wrappers for detailed suite reports -->

<xsl:template match="fio" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">fio</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="corruption" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">corruption</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="check_emptiness" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">check_emptiness</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="nfs_fio" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">nfs_fio</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="nfs_corruption" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">nfs_corruption</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="coreutils" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">coreutils</xsl:with-param>
        <xsl:with-param name="show-all-testsuites" select="false()" />
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_acceptance" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">disk_manager_acceptance</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_eternal" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">disk_manager_eternal</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_sync" mode="detailed">
    <xsl:call-template name="detailed">
        <xsl:with-param name="suite-kind">disk_manager_sync</xsl:with-param>
    </xsl:call-template>
</xsl:template>
<!-- boilerplate wrappers for brief suite reports -->

<xsl:template match="fio" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">fio</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="corruption" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">corruption</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="check_emptiness" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">check_emptiness</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="nfs_fio" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">nfs_fio</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="nfs_corruption" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">nfs_corruption</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="coreutils" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">coreutils</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_acceptance" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">disk_manager_acceptance</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_eternal" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">disk_manager_eternal</xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template match="disk_manager_sync" mode="brief">
    <xsl:call-template name="brief">
        <xsl:with-param name="suite-kind">disk_manager_sync</xsl:with-param>
    </xsl:call-template>
</xsl:template>
<!-- main template -->

<xsl:template match="index">
    <html>
        <xsl:call-template name="head"/>
        <body>
            <div style="margin-bottom: 20px">
                <h3>github CI</h3>
                <xsl:apply-templates select="tests"/>
            </div>
            <div style="border-top: 1px dashed black; margin-bottom: 20px">
                <h3>e2e brief</h3>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>NBS</h3>
                    <xsl:apply-templates select="fio" mode="brief"/>
                    <xsl:apply-templates select="corruption" mode="brief"/>
                    <xsl:apply-templates select="check_emptiness" mode="brief"/>
                </div>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>NFS</h3>
                    <xsl:apply-templates select="nfs_fio" mode="brief"/>
                    <xsl:apply-templates select="nfs_corruption" mode="brief"/>
                    <xsl:apply-templates select="xfs" mode="brief"/>
                    <xsl:apply-templates select="coreutils" mode="brief"/>
                </div>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>DM</h3>
                    <xsl:apply-templates select="disk_manager_acceptance" mode="brief"/>
                    <xsl:apply-templates select="disk_manager_eternal" mode="brief"/>
                    <xsl:apply-templates select="disk_manager_sync" mode="brief"/>
                </div>
            </div>
            <div style="border-top: 1px dashed black; margin-bottom: 20px">
                <h3>e2e detailed</h3>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>NBS</h3>
                    <xsl:apply-templates select="fio" mode="detailed"/>
                    <xsl:apply-templates select="corruption" mode="detailed"/>
                    <xsl:apply-templates select="check_emptiness" mode="detailed"/>
                </div>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>NFS</h3>
                    <xsl:apply-templates select="nfs_fio" mode="detailed"/>
                    <xsl:apply-templates select="nfs_corruption" mode="detailed"/>
                    <xsl:apply-templates select="xfs" mode="detailed"/>
                    <xsl:apply-templates select="coreutils" mode="detailed"/>
                </div>
                <div style="border: 1px solid black; margin-bottom: 20px; padding: 10px">
                    <h3>DM</h3>
                    <xsl:apply-templates select="disk_manager_acceptance" mode="detailed"/>
                    <xsl:apply-templates select="disk_manager_eternal" mode="detailed"/>
                    <xsl:apply-templates select="disk_manager_sync" mode="detailed"/>
                </div>
            </div>
        </body>
    </html>
</xsl:template>

</xsl:stylesheet>
