/*******************************************************************************

Protobuf reflection renders local config saved before CMS, private YAML and
effective configuration.
An explicit list of secret field names controls redaction for all value types.
Repeated values are aligned by index and map values by key across sources.

*******************************************************************************/

#include "configs_manager_renderer.h"

#include <cloud/storage/core/config/markers.pb.h>

#include <contrib/ydb/core/cms/console/http.h>

#include <library/cpp/html/pcdata/pcdata.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/strbuf.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/system/compiler.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <array>
#include <optional>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;

using TMessages = std::array<const Message*, 3>;

// Rendering inputs borrowed by field helpers for one configuration section.
// Create a context per section; referenced data must outlive the rendering
// call.
struct TRenderContext
{
    // Live ICB controls borrowed from the current configuration.
    const NStorage::TStorageConfigControls& Controls;

    // Rejected runtime paths borrowed from the renderer's accepted publication.
    const NConfig::TRuntimeConfigDiagnostics& Diagnostics;

    // ICB column applicability; true only for StorageService.
    bool ShowIcb;
};

// Secret field names whose values are hidden for every protobuf type.
constexpr TStringBuf SensitiveFieldNames[] = {
    "AuthToken",
    "NodeRegistrationToken",
};

// Format the accepted configuration update status for monitoring.
TStringBuf GetUpdateStatusName(EConfigUpdateStatus status)
{
    switch (status) {
        case EConfigUpdateStatus::Startup:
            return "startup";
        case EConfigUpdateStatus::Runtime:
            return "runtime";
        case EConfigUpdateStatus::RuntimeUnchanged:
            return "runtime (unchanged)";
    }
    return "<error>";
}

// Build the Effective column from the current configuration sections.
// Include live ICB overrides from the StorageConfig effective proto.
NProto::TBlockstoreConfig BuildEffectiveConfig(
    const IBlockstoreConfig& currentConfig)
{
    NProto::TBlockstoreConfig config;
    config.MutableServer()->CopyFrom(
        currentConfig.GetServerConfig()->GetAppConfig());
    config.MutableStorageService()->CopyFrom(
        currentConfig.GetStorageConfig()->GetEffectiveStorageConfigProto());
    config.MutableFeatures()->CopyFrom(
        currentConfig.GetFeaturesConfig()->GetConfigProto());
    config.MutableDiagnostics()->CopyFrom(
        currentConfig.GetDiagnosticsConfig()->GetConfigProto());
    config.MutableDiscoveryService()->CopyFrom(
        *currentConfig.GetDiscoveryServiceConfig()->GetConfig());
    config.MutableEndpoint()->CopyFrom(
        currentConfig.GetEndpointConfig()->GetConfigProto());
    config.MutableDiskAgent()->CopyFrom(
        currentConfig.GetDiskAgentConfig()->GetConfigProto());
    config.MutableDiskRegistryProxy()->CopyFrom(
        currentConfig.GetDiskRegistryProxyConfig()->GetConfigProto());
    config.MutableSpdkEnv()->CopyFrom(
        currentConfig.GetSpdkEnvConfig()->GetConfigProto());
    config.MutableRdma()->CopyFrom(
        currentConfig.GetRdmaConfig()->GetConfigProto());
    config.MutableYdbStats()->CopyFrom(
        currentConfig.GetYdbStatsConfig()->GetConfigProto());
    config.MutableLogbroker()->CopyFrom(
        *currentConfig.GetLogbrokerConfig()->GetConfig());
    config.MutableNotify()->CopyFrom(
        *currentConfig.GetNotifyConfig()->GetConfig());
    config.MutableIamClient()->CopyFrom(
        currentConfig.GetIamClientConfig()->GetIamServiceConfig());
    config.MutableKmsClient()->CopyFrom(*currentConfig.GetKmsClientConfig());
    config.MutableComputeClient()->CopyFrom(
        *currentConfig.GetComputeClientConfig());
    config.MutableRootKms()->CopyFrom(*currentConfig.GetRootKmsConfig());
    config.MutableCells()->CopyFrom(
        currentConfig.GetCellsConfig()->GetCellsConfig());
    config.MutableLocalNVMe()->CopyFrom(
        currentConfig.GetLocalNVMeConfig()->GetConfigProto());
    return config;
}

bool IsSensitive(const FieldDescriptor& field)
{
    return IsIn(SensitiveFieldNames, field.name());
}

// Check source presence, retaining scalar defaults inside existing map entries.
bool HasValue(
    const Message* message,
    const FieldDescriptor* field,
    int index = -1)
{
    if (!message) {
        return false;
    }

    const auto* reflection = message->GetReflection();
    return field->is_repeated()
               ? reflection->FieldSize(*message, field) > Max(index, 0)
               : field->containing_type()->options().map_entry() ||
                     reflection->HasField(*message, field);
}

const Message*
GetMessage(const Message* message, const FieldDescriptor* field, int index = -1)
{
    if (!HasValue(message, field, index)) {
        return nullptr;
    }
    const auto* reflection = message->GetReflection();
    return field->is_repeated()
               ? &reflection->GetRepeatedMessage(*message, field, index)
               : &reflection->GetMessage(*message, field);
}

// Format a singular or indexed value and redact explicitly named secrets.
std::optional<TString>
GetValue(const Message* message, const FieldDescriptor* field, int index = -1)
{
    if (!HasValue(message, field, index)) {
        return std::nullopt;
    }

    if (IsSensitive(*field)) {
        return TString("[redacted]");
    }

    TString value;
    google::protobuf::TextFormat::PrintFieldValueToString(
        *message,
        field,
        index,
        &value);
    return value;
}

void RenderCell(IOutputStream& out, const std::optional<TString>& value)
{
    HTML (out) {
        TABLED () {
            if (value) {
                out << EncodeHtmlPcdata(*value);
            } else {
                out << "&mdash;";
            }
        }
    }
}

// Match deferred schema paths to displayed fields and collection elements.
bool IsDeferred(
    TStringBuf path,
    const NConfig::TRuntimeConfigDiagnostics& diagnostics)
{
    for (const auto& [ignoredPath, reason]: diagnostics.IgnoredPaths) {
        Y_UNUSED(reason);
        // Collections are terminal schema paths; mark every displayed element.
        if (ignoredPath.EndsWith("[]")) {
            if (path.StartsWith(TStringBuf(ignoredPath).Chop(1))) {
                return true;
            }
        } else if (
            path == ignoredPath ||
            (path.StartsWith(ignoredPath) && path.size() > ignoredPath.size() &&
             path[ignoredPath.size()] == '.'))
        {
            return true;
        }
    }
    return false;
}

// Render one source row, including ICB cells only for the storage section.
void RenderField(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context,
    std::optional<TAtomicBase> icb = std::nullopt,
    int index = -1)
{
    HTML (out) {
        TABLER () {
            out << (IsDeferred(path, context.Diagnostics)
                        ? "<td class='config-dynamic-deferred'>"
                        : "<td>")
                << EncodeHtmlPcdata(path) << "</td>";
            RenderCell(out, GetValue(sources[0], field, index));
            RenderCell(out, GetValue(sources[1], field, index));
            if (context.ShowIcb) {
                if (field->options().GetExtension(
                        NCloud::NMarkers::AllowRuntimeUpdate))
                {
                    RenderCell(
                        out,
                        icb ? std::optional<TString>(
                                  IsSensitive(*field) ? TString("[redacted]")
                                                      : ToString(*icb))
                            : std::nullopt);
                } else {
                    out << "<td class='config-icb-unavailable'></td>";
                }
            }
            RenderCell(out, GetValue(sources[2], field, index));
        }
    }
    out << Endl;
}

// Counts for one section; collections contribute one parameter per field.
struct TSectionCounts
{
    // Parameters explicitly present in the private source.
    ui32 Dynamic = 0;

    // Rejected runtime changes, including deferred removals.
    ui32 PendingRestart = 0;

    // Storage fields with a current operator override.
    ui32 Icb = 0;
};

// Count rejected paths within one section without treating ICB as a rejection.
ui32 CountPendingRestart(
    TStringBuf section,
    const NConfig::TRuntimeConfigDiagnostics& diagnostics)
{
    const TString prefix = TStringBuilder() << section << '.';
    ui32 count = 0;
    for (const auto& [path, reason]: diagnostics.IgnoredPaths) {
        Y_UNUSED(reason);
        count += path == section || path.StartsWith(prefix);
    }
    return count;
}

// Render source fields in schema order and count each collection once.
bool RenderMessage(
    IOutputStream& out,
    const TMessages& sources,
    const google::protobuf::Descriptor* descriptor,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts);

// Render present message children while retaining their section counters.
bool RenderNestedMessage(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts,
    int index = -1)
{
    const TMessages children = {
        GetMessage(sources[0], field, index),
        GetMessage(sources[1], field, index),
        GetMessage(sources[2], field, index),
    };
    // Stop at absent messages before descending into their schemas.
    if (!children[0] && !children[1] && !children[2]) {
        return false;
    }
    return RenderMessage(
        out,
        children,
        field->message_type(),
        path,
        context,
        counts);
}

// Render one collection element without adding its children to section totals.
void RenderCollectionElement(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context,
    int index = -1)
{
    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
        TSectionCounts elementCounts;
        if (RenderNestedMessage(
                out,
                sources,
                field,
                path,
                context,
                elementCounts,
                index))
        {
            return;
        }
    }

    // Retain scalar values and explicitly present empty messages as source
    // rows.
    RenderField(out, sources, field, path, context, std::nullopt, index);
}

// Align map entries by key independently of each source's iteration order.
TMap<TString, TMessages> GetMapEntries(
    const TMessages& sources,
    const FieldDescriptor* field)
{
    const auto* keyField = field->message_type()->map_key();
    TMap<TString, TMessages> entries;
    for (size_t source = 0; source != sources.size(); ++source) {
        const auto* message = sources[source];
        if (!message) {
            continue;
        }
        const auto* reflection = message->GetReflection();
        const int size = reflection->FieldSize(*message, field);
        for (int j = 0; j != size; ++j) {
            const auto& entry =
                reflection->GetRepeatedMessage(*message, field, j);
            const auto key =
                keyField->cpp_type() == FieldDescriptor::CPPTYPE_STRING
                    ? TString(entry.GetReflection()->GetString(entry, keyField))
                    : *GetValue(&entry, keyField);
            entries[key][source] = &entry;
        }
    }
    return entries;
}

// Render aligned map values using escaped keys in parameter names.
bool RenderMapField(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context)
{
    const auto entries = GetMapEntries(sources, field);
    const auto* valueField = field->message_type()->map_value();
    for (const auto& [key, entriesBySource]: entries) {
        const TString entryPath = TStringBuilder()
                                  << path << "[\"" << EscapeC(key) << "\"]";
        RenderCollectionElement(
            out,
            entriesBySource,
            valueField,
            entryPath,
            context);
    }
    return !entries.empty();
}

// Render every repeated element at its source index, including missing cells.
bool RenderRepeatedField(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context)
{
    // Cover the longest source list so trailing elements remain visible.
    int size = 0;
    for (const auto* message: sources) {
        if (message) {
            size =
                Max(size, message->GetReflection()->FieldSize(*message, field));
        }
    }

    // Preserve each source's indices rather than pairing elements by value.
    for (int j = 0; j != size; ++j) {
        const TString itemPath = TStringBuilder() << path << '[' << j << ']';
        RenderCollectionElement(out, sources, field, itemPath, context, j);
    }
    return size != 0;
}

// Render one present scalar and count its dynamic source and direct ICB
// override.
bool RenderScalarField(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts)
{
    // Read ICB only for direct StorageService fields to avoid nested name
    // clashes.
    std::optional<TAtomicBase> icb;
    if (context.ShowIcb &&
        field->containing_type() == NProto::TStorageServiceConfig::descriptor())
    {
        icb = context.Controls.GetOverride(field->name());
    }

    // Omit unset fields unless a live ICB override supplies their value.
    if (!HasValue(sources[0], field) && !HasValue(sources[1], field) &&
        !HasValue(sources[2], field) && !icb)
    {
        return false;
    }

    // Count explicitly configured values and current operator overrides.
    counts.Dynamic += HasValue(sources[1], field);
    counts.Icb += icb.has_value();
    RenderField(out, sources, field, path, context, icb);
    return true;
}

// Dispatch fields by protobuf shape, counting each collection as one parameter.
bool RenderMessage(
    IOutputStream& out,
    const TMessages& sources,
    const google::protobuf::Descriptor* descriptor,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts)
{
    bool rendered = false;
    for (int i = 0; i != descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        const TString fieldPath = TStringBuilder()
                                  << path << '.' << field->name();
        if (field->is_repeated()) {
            counts.Dynamic += HasValue(sources[1], field);
            rendered |=
                field->is_map()
                    ? RenderMapField(out, sources, field, fieldPath, context)
                    : RenderRepeatedField(
                          out,
                          sources,
                          field,
                          fieldPath,
                          context);
        } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            rendered |= RenderNestedMessage(
                out,
                sources,
                field,
                fieldPath,
                context,
                counts);
        } else {
            rendered |= RenderScalarField(
                out,
                sources,
                field,
                fieldPath,
                context,
                counts);
        }
    }
    return rendered;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Render the supplied sources and current configuration, escaping HTML and
// hiding values selected by the credential checks. Include ICB overrides only
// for direct StorageService fields.
TString TBlockstoreConfigRenderer::RenderHtml(
    const IBlockstoreConfig& config,
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    const NStorage::TStorageConfigControls& controls) const
{
    const auto effectiveConfig = BuildEffectiveConfig(config);

    TStringStream out;
    HTML (out) {
        HEAD () {
            out << R"(<style>
.config-zero { color: #000; }
.config-dynamic-total, .config-present { color: #008000; }
.config-absent { color: #666; }
.config-summary td:first-child { width: 1%; white-space: nowrap; }
.config-dynamic-deferred { color: #b35a00; }
.config-deferred-hint { color: #000; }
.config-icb, .config-update-rejected { color: #c00; }
.config-icb-unavailable { background-color: #eee; }
.config-actions { display: flex; align-items: center; justify-content: space-between;
                  flex-wrap: wrap; gap: 1em; margin-bottom: 1em; }
.config-search { display: flex; align-items: center; margin: 0; font-weight: normal; }
.config-search input { width: 20em; margin-left: .5em; }
.config-toggle-actions { margin-left: auto; white-space: nowrap; }
.config-details > td { padding: 0; border-top: 0; }
.config-parameters { table-layout: fixed; }
.config-parameters th:first-child,
.config-parameters td:first-child { white-space: nowrap; }
.config-parameters td { overflow-wrap: anywhere; }
</style>
<script>
$(function () {
    var sections = $('#config-sections .config-section');
    var expandedSections = null;
    sections.collapse({toggle: false});

    // Measure all parameter names, including names in collapsed sections.
    var measurement = $('<table class="table table-condensed table-bordered">' +
                        '<tbody></tbody></table>').css({
        position: 'fixed', visibility: 'hidden', width: 'auto', maxWidth: 'none'
    });
    sections.find('.config-parameters tr > :first-child').each(function () {
        $('<tr>').append($(this).clone().css('white-space', 'nowrap'))
            .appendTo(measurement.children('tbody'));
    });
    measurement.appendTo(document.body);
    var parameterWidth = Math.ceil(measurement.outerWidth());
    measurement.remove();

    // Keep the name width shared; split remaining space equally with fixed layout.
    sections.find('.config-parameters th:first-child').css('width', parameterWidth);

    // Apply the latest request after an active Bootstrap transition finishes.
    function collapseSection(section, action) {
        section.off('.configSearch');
        if (section.data('bs.collapse').transitioning) {
            section.one(
                'shown.bs.collapse.configSearch hidden.bs.collapse.configSearch',
                function () {
                    section.off('.configSearch').collapse(action);
                });
        } else {
            section.collapse(action);
        }
    }

    $('#config-collapse-all, #config-expand-all').on('click', function (event) {
        event.preventDefault();
        var action = this.id === 'config-expand-all' ? 'show' : 'hide';
        sections.each(function () {
            var section = $(this);
            if (section.closest('tr').css('display') !== 'none') {
                collapseSection(section, action);
            }
        });
    });

    $('#config-parameter-search').on('input', function () {
        var query = $.trim(this.value).toLowerCase();
        if (query.length < 3) {
            if (expandedSections === null) {
                return;
            }
            query = '';
        }
        if (query && expandedSections === null) {
            expandedSections = sections.filter('.in').map(function () {
                return this.id;
            }).get();
        }

        // Filter parameter names while retaining source values and counters.
        var matches = 0;
        sections.each(function () {
            var section = $(this);
            var sectionMatches = 0;
            section.find('tbody > tr').each(function () {
                var row = $(this);
                var name = row.children('td').first().text().toLowerCase();
                var visible = name.indexOf(query) !== -1;
                row.toggle(visible);
                sectionMatches += visible ? 1 : 0;
            });
            matches += sectionMatches;
            var details = section.closest('tr');
            details.toggle(sectionMatches > 0);
            details.prev().toggle(sectionMatches > 0);

            // Reveal matches, then restore section state when search is cleared.
            if (query && sectionMatches) {
                collapseSection(section, 'show');
            } else if (!query && expandedSections !== null) {
                collapseSection(section,
                    expandedSections.indexOf(this.id) !== -1 ? 'show' : 'hide');
            }
        });
        $('#config-search-empty').toggle(Boolean(query) && matches === 0);
        if (!query) {
            expandedSections = null;
        }
    });
});
</script>)" << Endl;
        }
        NKikimr::NConsole::NHttp::OutputStyles(out);

        DIV_CLASS ("container") {
            TAG (TH3) {
                out << "Config";
            }
            TABLE_CLASS ("table table-condensed config-summary") {
                TABLEBODY () {
                    TABLER () {
                        TABLED () {
                            out << "Last update";
                        }
                        TABLED () {
                            out << Data.LastUpdateTime.ToString() << ", "
                                << EncodeHtmlPcdata(
                                       GetUpdateStatusName(Data.UpdateStatus));
                            if (Data.RejectedUpdateReason) {
                                out << " <span class='config-update-rejected'>"
                                       "- rejected: "
                                    << EncodeHtmlPcdata(
                                           Data.RejectedUpdateReason)
                                    << "</span>";
                            }
                        }
                    }
                    TABLER () {
                        TABLED () {
                            out << "Dynamic config";
                        }
                        TABLED () {
                            out << "<span class='"
                                << (Data.DynamicConfigPresent ? "config-present"
                                                              : "config-absent")
                                << "'>"
                                << (Data.DynamicConfigPresent ? "present"
                                                              : "absent")
                                << "</span>";
                        }
                    }
                }
            }

            out << "<div class='config-actions'>"
                << "<label class='config-search' for='config-parameter-search'>"
                << "Search parameter: "
                << "<input id='config-parameter-search' type='search' "
                << "class='form-control input-sm' "
                << "placeholder='At least 3 characters'></label>"
                << "<div class='config-toggle-actions'>"
                << "<a href='#' id='config-collapse-all' role='button' "
                << "class='btn btn-default btn-sm'>Collapse all</a> "
                << "<a href='#' id='config-expand-all' role='button' "
                << "class='btn btn-default btn-sm'>Expand all</a>"
                << "</div></div>";
            if (!Data.RuntimeDiagnostics.IgnoredPaths.empty()) {
                out << "<p class='config-deferred-hint'>"
                    << "<span class='config-dynamic-deferred'>Deferred</span> "
                    << "parameters cannot be updated at runtime and take "
                    << "effect after restart.</p>";
            }
            out << "<p id='config-search-empty' role='status' "
                << "style='display: none'>No matching parameters.</p>"
                << "<table class='table table-condensed' id='config-sections'>"
                << "<thead><tr><th></th><th>Dynamic</th><th>ICB</th>"
                << "</tr></thead><tbody>";

            const auto* descriptor = NProto::TBlockstoreConfig::descriptor();
            const auto* staticReflection = staticConfig.GetReflection();
            const auto* dynamicReflection = dynamicConfig.GetReflection();
            const auto* effectiveReflection = effectiveConfig.GetReflection();

            for (int i = 0; i != descriptor->field_count(); ++i) {
                const auto* section = descriptor->field(i);
                const bool showIcb = section->name() == "StorageService";
                const Message* staticMessage =
                    staticReflection->HasField(staticConfig, section)
                        ? &staticReflection->GetMessage(staticConfig, section)
                        : nullptr;
                const Message* dynamicMessage =
                    dynamicReflection->HasField(dynamicConfig, section)
                        ? &dynamicReflection->GetMessage(dynamicConfig, section)
                        : nullptr;
                const Message* effectiveMessage =
                    effectiveReflection->HasField(effectiveConfig, section)
                        ? &effectiveReflection->GetMessage(
                              effectiveConfig,
                              section)
                        : nullptr;

                TStringStream rows;
                TSectionCounts counts;
                const TRenderContext context{
                    .Controls = controls,
                    .Diagnostics = Data.RuntimeDiagnostics,
                    .ShowIcb = showIcb,
                };
                if (!RenderMessage(
                        rows,
                        {staticMessage, dynamicMessage, effectiveMessage},
                        section->message_type(),
                        section->name(),
                        context,
                        counts))
                {
                    continue;
                }

                const TString target = TStringBuilder()
                                       << "config-section-" << section->name();
                counts.PendingRestart = CountPendingRestart(
                    section->name(),
                    Data.RuntimeDiagnostics);

                // Keep counters outside the collapsed details for this section.
                out << "<tr><td><a href='#" << target
                    << "' class='collapse-ref collapsed' "
                    << "data-toggle='collapse' "
                    << "data-target='#" << target
                    << "' aria-expanded='false' aria-controls='" << target
                    << "'>" << section->name() << "</a></td><td>"
                    << "<span class='"
                    << (counts.Dynamic ? "config-dynamic-total" : "config-zero")
                    << "'>";
                if (counts.Dynamic) {
                    out << counts.Dynamic;
                } else {
                    out << "&mdash;";
                }
                out << "</span>";
                if (counts.PendingRestart) {
                    out << " / <span class='config-dynamic-deferred'>deferred: "
                        << counts.PendingRestart << "</span>";
                }
                out << "</td><td><span class='"
                    << (counts.Icb ? "config-icb" : "config-zero") << "'>";
                if (counts.Icb) {
                    out << counts.Icb;
                } else {
                    out << "&mdash;";
                }
                out << "</span></td></tr>"
                    << "<tr class='config-details'><td colspan='3'>"
                    << "<div id='" << target
                    << "' class='collapse config-section'>";
                {
                    TABLE_CLASS (
                        "table table-condensed table-bordered "
                        "config-parameters")
                    {
                        TABLEHEAD () {
                            TABLER () {
                                for (const TStringBuf title:
                                     {
                                         "Parameter",
                                         "Static",
                                         "Dynamic",
                                         "ICB",
                                         "Effective",
                                     })
                                {
                                    if (title == "ICB" && !showIcb) {
                                        continue;
                                    }
                                    TABLEH () {
                                        out << title;
                                    }
                                }
                            }
                        }
                        TABLEBODY () {
                            out << rows.Str();
                        }
                    }
                }
                out << "</div></td></tr>" << Endl;
            }
            out << "</tbody></table>";
        }
    }

    return out.Str();
}

}   // namespace NCloud::NBlockStore
