/*******************************************************************************

Protobuf reflection renders local config saved before CMS, private YAML and
effective configuration.
An explicit list of secret field names controls redaction for all value types.
Repeated values are aligned by index and map values by key across sources.

*******************************************************************************/

#include "configs_manager_renderer.h"

#include "configs_manager_renderer_impl.h"

#include <cloud/storage/core/config/markers.pb.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/strbuf.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <array>
#include <optional>
#include <utility>

namespace NCloud::NBlockStore {

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

NProto::TBlockstoreConfig BuildEffectiveConfig(
    const IBlockstoreConfig& currentConfig)
{
    NProto::TBlockstoreConfig config;

    *config.MutableServer() = currentConfig.GetServerConfig()->GetAppConfig();
    *config.MutableStorageService() =
        currentConfig.GetStorageConfig()->GetEffectiveStorageConfigProto();
    *config.MutableFeatures() =
        currentConfig.GetFeaturesConfig()->GetConfigProto();
    *config.MutableDiagnostics() =
        currentConfig.GetDiagnosticsConfig()->GetConfigProto();
    *config.MutableDiscoveryService() =
        *currentConfig.GetDiscoveryServiceConfig()->GetConfig();
    *config.MutableEndpoint() =
        currentConfig.GetEndpointConfig()->GetConfigProto();
    *config.MutableDiskAgent() =
        currentConfig.GetDiskAgentConfig()->GetConfigProto();
    *config.MutableDiskRegistryProxy() =
        currentConfig.GetDiskRegistryProxyConfig()->GetConfigProto();
    *config.MutableSpdkEnv() =
        currentConfig.GetSpdkEnvConfig()->GetConfigProto();
    *config.MutableRdma() = currentConfig.GetRdmaConfig()->GetConfigProto();
    *config.MutableYdbStats() =
        currentConfig.GetYdbStatsConfig()->GetConfigProto();
    *config.MutableLogbroker() =
        *currentConfig.GetLogbrokerConfig()->GetConfig();
    *config.MutableNotify() = *currentConfig.GetNotifyConfig()->GetConfig();
    *config.MutableIamClient() =
        currentConfig.GetIamClientConfig()->GetIamServiceConfig();
    *config.MutableKmsClient() = *currentConfig.GetKmsClientConfig();
    *config.MutableComputeClient() = *currentConfig.GetComputeClientConfig();
    *config.MutableRootKms() = *currentConfig.GetRootKmsConfig();
    *config.MutableCells() = currentConfig.GetCellsConfig()->GetCellsConfig();
    *config.MutableLocalNVMe() =
        currentConfig.GetLocalNVMeConfig()->GetConfigProto();

    return config;
}

}   // namespace NPrivate

namespace {

////////////////////////////////////////////////////////////////////////////////

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using NConfig::EConfigIgnoreReason;

// Source roles used consistently in field rows and current CMS counters.
enum EConfigSource : size_t
{
    StaticSource,
    StartupDynamicSource,
    RuntimeDynamicSource,
    EffectiveSource,
    ConfigSourceCount,
};

using TMessages = std::array<const Message*, ConfigSourceCount>;
using TOverrideInfo = NStorage::TStorageConfigControls::TOverrideInfo;

// Rendering inputs borrowed by field helpers for one configuration section.
// Create a context per section; referenced data must outlive the rendering
// call.
struct TRenderContext
{
    // Live ICB controls borrowed from the current configuration.
    const NStorage::TStorageConfigControls& Controls;

    // Ignored values and rejected changes for the accepted publication.
    const NConfig::TConfigPreparationDiagnostics& Diagnostics;

    // Static-only paths from the unchanged startup source report.
    const NConfig::TConfigPreparationDiagnostics&
        StartupDynamicConfigNormalizationDiagnostics;

    // Accepted CMS source whose parameters contribute to section counters.
    EConfigSource CurrentDynamicSource;

    // ICB column applicability; true only for StorageService.
    bool ShowIcb;

    // A rejected change to an enclosing oneof group.
    bool DeferredByOneof = false;
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
        case EConfigUpdateStatus::StartupPartiallyApplied:
            return "startup (partially applied)";
        case EConfigUpdateStatus::Runtime:
            return "runtime";
        case EConfigUpdateStatus::RuntimeUnchanged:
            return "runtime (unchanged)";
        case EConfigUpdateStatus::RuntimePartiallyApplied:
            return "runtime (partially applied)";
    }
    return "<error>";
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

// Format enum names with their numbers and label undeclared values.
TString FormatEnumValue(
    const google::protobuf::EnumDescriptor& enumType,
    i64 value)
{
    const auto* enumValue =
        std::in_range<int>(value)
            ? enumType.FindValueByNumber(static_cast<int>(value))
            : nullptr;
    return enumValue
               ? (TStringBuilder() << enumValue->name() << " (" << value << ')')
               : ToString(value) + " (unknown enum value)";
}

// Show bool and enum names with numbers; keep other ICB values numeric.
TString FormatIcbValue(const FieldDescriptor& field, i64 value)
{
    if (field.cpp_type() == FieldDescriptor::CPPTYPE_BOOL) {
        return TStringBuilder()
               << (value ? "true" : "false") << " (" << value << ')';
    }

    return field.cpp_type() == FieldDescriptor::CPPTYPE_ENUM
               ? FormatEnumValue(*field.enum_type(), value)
               : ToString(value);
}

// Format scalars, mark empty messages and redact explicitly named secrets.
std::optional<TString>
GetValue(const Message* message, const FieldDescriptor* field, int index = -1)
{
    if (!HasValue(message, field, index)) {
        return std::nullopt;
    }

    if (IsSensitive(*field)) {
        return TString("[redacted]");
    }

    if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
        const auto* nested = GetMessage(message, field, index);
        return nested && !HasKnownFields(*nested)
                   ? std::optional<TString>("{}")
                   : std::nullopt;
    }

    if (field->cpp_type() == FieldDescriptor::CPPTYPE_ENUM) {
        const auto* reflection = message->GetReflection();
        const auto value =
            field->is_repeated()
                ? reflection->GetRepeatedEnumValue(*message, field, index)
                : reflection->GetEnumValue(*message, field);
        return FormatEnumValue(*field->enum_type(), value);
    }

    TString value;
    google::protobuf::TextFormat::PrintFieldValueToString(
        *message,
        field,
        index,
        &value);
    return value;
}

// Format the captured ICB value and explain why an invalid enum is ignored.
TString FormatIcbOverride(
    const FieldDescriptor& field,
    const TOverrideInfo& info)
{
    return info.IsValid
               ? FormatIcbValue(field, info.Value)
               : ToString(info.Value) + " (unknown enum value; ignored)";
}

// Render one source value cell and its optional ignored static-only label.
void RenderCell(
    IOutputStream& out,
    const std::optional<TString>& value,
    bool ignoredFromDynamic)
{
    HTML (out) {
        // Value cell in a parameter comparison row.
        TABLED () {
            if (value) {
                // Source value or the redaction placeholder for a secret.
                out << EncodeHtmlPcdata(*value);
                if (ignoredFromDynamic) {
                    // Line break between the value and its ignored label.
                    out << "<br>";

                    // Ignored static-only label below the Dynamic source value.
                    out << "<span class='config-dynamic-ignored'>"
                           "static-only, ignored</span>";
                }
            } else {
                // Dash for a value absent from this source.
                out << "&mdash;";
            }
        }
    }
}

// Check whether an ignored change can take effect after restart.
bool IsDeferredReason(EConfigIgnoreReason reason)
{
    return reason != EConfigIgnoreReason::StaticOnly;
}

// Match a schema path to its displayed field, children, or collection elements.
bool MatchesIgnoredPath(TStringBuf path, TStringBuf ignoredPath)
{
    if (ignoredPath.EndsWith("[]")) {
        return path.StartsWith(ignoredPath.Chop(1));
    }
    return path == ignoredPath ||
           (path.StartsWith(ignoredPath) && path.size() > ignoredPath.size() &&
            path[ignoredPath.size()] == '.');
}

// Match deferred schema paths to displayed fields and collection elements.
bool IsDeferred(
    TStringBuf path,
    const NConfig::TConfigPreparationDiagnostics& diagnostics)
{
    for (const auto& [ignoredPath, reason]: diagnostics.IgnoredPaths) {
        if (IsDeferredReason(reason) && MatchesIgnoredPath(path, ignoredPath)) {
            return true;
        }
    }
    return false;
}

// Match static-only paths to source fields and their children.
bool IsStaticOnly(
    TStringBuf path,
    const NConfig::TConfigPreparationDiagnostics& diagnostics)
{
    for (const auto& [ignoredPath, reason]: diagnostics.IgnoredPaths) {
        if (reason == EConfigIgnoreReason::StaticOnly &&
            MatchesIgnoredPath(path, ignoredPath))
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
    std::optional<TOverrideInfo> icb = std::nullopt,
    int index = -1)
{
    HTML (out) {
        // Parameter row with its name and all applicable source values.
        TABLER () {
            // Parameter-name cell; highlight changes that require a restart.
            out << (context.DeferredByOneof ||
                            IsDeferred(path, context.Diagnostics)
                        ? "<td class='config-dynamic-deferred'>"
                        : "<td>")
                << EncodeHtmlPcdata(path) << "</td>";

            // Static, startup Dynamic and runtime Dynamic value cells.
            for (const auto source:
                 {StaticSource, StartupDynamicSource, RuntimeDynamicSource})
            {
                bool ignoredFromDynamic;

                if (source == StartupDynamicSource) {
                    ignoredFromDynamic = IsStaticOnly(
                        path,
                        context.StartupDynamicConfigNormalizationDiagnostics);
                } else if (source == RuntimeDynamicSource) {
                    ignoredFromDynamic =
                        IsStaticOnly(path, context.Diagnostics);
                } else {
                    ignoredFromDynamic = false;
                }

                RenderCell(
                    out,
                    GetValue(sources[source], field, index),
                    /*ignoredFromDynamic=*/ignoredFromDynamic);
            }

            if (context.ShowIcb) {
                if (field->options().GetExtension(
                        NCloud::NMarkers::AllowRuntimeUpdate))
                {
                    // ICB override cell; use a dash when no override is set.
                    RenderCell(
                        out,
                        icb ? std::optional<TString>(
                                  IsSensitive(*field)
                                      ? TString("[redacted]")
                                      : FormatIcbOverride(*field, *icb))
                            : std::nullopt,
                        /*ignoredFromDynamic=*/false);
                } else {
                    // Unavailable ICB cell for fields without runtime updates.
                    out << "<td class='config-icb-unavailable'></td>";
                }
            }

            // Effective value cell after source merging and ICB overrides.
            RenderCell(
                out,
                GetValue(sources[EffectiveSource], field, index),
                /*ignoredFromDynamic=*/false);
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

    // Static-only parameters removed from the current private source.
    ui32 Ignored = 0;

    // Storage fields with a current operator override.
    ui32 Icb = 0;
};

// Count deferred and static-only parameters by schema path within one section.
void CountIgnoredParameters(
    TStringBuf section,
    const NConfig::TConfigPreparationDiagnostics& diagnostics,
    TSectionCounts& counts)
{
    const TString prefix = TStringBuilder() << section << '.';
    for (const auto& [path, reason]: diagnostics.IgnoredPaths) {
        if (path != section && !path.StartsWith(prefix)) {
            continue;
        }
        if (reason == EConfigIgnoreReason::StaticOnly) {
            ++counts.Ignored;
        } else if (IsDeferredReason(reason)) {
            ++counts.PendingRestart;
        }
    }
}

// Render source fields in schema order and count each collection once.
bool RenderMessage(
    IOutputStream& out,
    const TMessages& sources,
    const google::protobuf::Descriptor* descriptor,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts);

// Render message fields and show explicitly empty messages as separate rows.
bool RenderNestedMessage(
    IOutputStream& out,
    const TMessages& sources,
    const FieldDescriptor* field,
    TStringBuf path,
    const TRenderContext& context,
    TSectionCounts& counts,
    int index = -1)
{
    TMessages children;
    bool hasEmptyMessage = false;
    for (size_t source = 0; source != sources.size(); ++source) {
        children[source] = GetMessage(sources[source], field, index);
        // Ignore empty top-level sections created by BuildEffectiveConfig.
        hasEmptyMessage |= children[source] &&
                           !HasKnownFields(*children[source]) &&
                           (source != EffectiveSource ||
                            field->containing_type() !=
                                NProto::TBlockstoreConfig::descriptor());
    }
    // Stop at absent messages before descending into their schemas.
    if (AllOf(children, [](const auto* child) { return !child; })) {
        return false;
    }

    // Retain empty source cells even when another source has populated fields.
    if (hasEmptyMessage) {
        const auto* dynamicMessage = children[context.CurrentDynamicSource];
        counts.Dynamic += dynamicMessage && !HasKnownFields(*dynamicMessage);
        RenderField(out, sources, field, path, context, std::nullopt, index);
    }

    const bool rendered = RenderMessage(
        out,
        children,
        field->message_type(),
        path,
        context,
        counts);
    return rendered || hasEmptyMessage;
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
        RenderNestedMessage(
            out,
            sources,
            field,
            path,
            context,
            elementCounts,
            index);
    } else {
        RenderField(out, sources, field, path, context, std::nullopt, index);
    }
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
    std::optional<TOverrideInfo> icb;
    if (context.ShowIcb &&
        field->containing_type() == NProto::TStorageServiceConfig::descriptor())
    {
        icb = context.Controls.GetOverrideInfo(field->name());
    }

    // Keep a row for any explicit ICB override, including rejected values.
    if (!icb &&
        AllOf(
            sources,
            [field](const auto* source) { return !HasValue(source, field); }))
    {
        return false;
    }

    // Count explicitly configured values and current operator overrides.
    counts.Dynamic += HasValue(sources[context.CurrentDynamicSource], field);
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

        // Match oneof diagnostics by the group name, retaining member paths.
        auto fieldContext = context;
        if (const auto* oneof = field->real_containing_oneof()) {
            const TString groupPath = TStringBuilder()
                                      << path << '.' << oneof->name();
            const auto it = context.Diagnostics.IgnoredPaths.find(groupPath);
            fieldContext.DeferredByOneof |=
                it != context.Diagnostics.IgnoredPaths.end() &&
                IsDeferredReason(it->second);
        }

        if (field->is_repeated()) {
            counts.Dynamic +=
                HasValue(sources[context.CurrentDynamicSource], field);
            rendered |= field->is_map() ? RenderMapField(
                                              out,
                                              sources,
                                              field,
                                              fieldPath,
                                              fieldContext)
                                        : RenderRepeatedField(
                                              out,
                                              sources,
                                              field,
                                              fieldPath,
                                              fieldContext);
        } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            rendered |= RenderNestedMessage(
                out,
                sources,
                field,
                fieldPath,
                fieldContext,
                counts);
        } else {
            rendered |= RenderScalarField(
                out,
                sources,
                field,
                fieldPath,
                fieldContext,
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
    const NProto::TBlockstoreConfig& dynamicConfig) const
{
    const auto& controls = *config.GetStorageConfig()->GetControls();
    const auto effectiveConfig = NPrivate::BuildEffectiveConfig(config);
    const bool hasRuntimeUpdate =
        Data.UpdateStatus != EConfigUpdateStatus::Startup &&
        Data.UpdateStatus != EConfigUpdateStatus::StartupPartiallyApplied;
    const TMessages sources = {
        &staticConfig,
        &Data.StartupDynamicConfig,
        hasRuntimeUpdate ? &dynamicConfig : nullptr,
        &effectiveConfig,
    };

    TStringStream out;
    HTML (out) {
        // Page styles for metadata, controls, counters and value tables.
        out << R"(<style>
.collapse-ref { cursor: pointer; }
.config-zero { color: #000; }
.config-dynamic-total, .config-present { color: #008000; }
.config-absent { color: #666; }
.config-summary td:first-child { width: 1%; white-space: nowrap; }
.config-dynamic-deferred { color: #b35a00; }
.config-deferred-hint { color: #000; }
.config-dynamic-ignored, .config-icb, .config-update-rejected { color: #c00; }
.config-icb-unavailable { background-color: #eee; }
.config-actions {
    display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 1em; margin-bottom: 1em;
}
.config-search {
    display: flex; align-items: center; margin: 0; font-weight: normal;
}
.config-search input { width: 20em; margin-left: .5em; }
.config-toggle-actions { margin-left: auto; white-space: nowrap; }
.config-details > td { padding: 0; border-top: 0; }
.config-parameters { table-layout: fixed; }
.config-parameters th:first-child,
.config-parameters td:first-child { white-space: nowrap; }
.config-parameters td { overflow-wrap: anywhere; }
</style>
)";

        // Section controls, parameter search and source-column widths.
        out << R"(<script>
$(function () {
    var sections = $('#config-sections .config-section');

    // Keep the IDs of sections open before searching;
    // null means no saved state.
    var expandedSections = null;

    // Initialize Bootstrap controls without changing which sections are open.
    sections.collapse({toggle: false});

    // Measure all parameter-name cells, including those in collapsed sections,
    // to give the name columns the same width across all section tables.
    var measurement = $('<table class="table table-condensed table-bordered">' +
                        '<tbody></tbody></table>').css({
        position: 'fixed', visibility: 'hidden', width: 'auto', maxWidth: 'none'
    });

    // Copy the name cells without wrapping so their full text
    // determines the width.
    sections.find('.config-parameters tr > :first-child').each(function () {
        $('<tr>').append($(this).clone().css('white-space', 'nowrap'))
            .appendTo(measurement.children('tbody'));
    });

    // Attach the hidden table so the browser can measure it, then remove it
    // after reading the width without affecting the visible page layout.
    measurement.appendTo(document.body);
    var parameterWidth = Math.ceil(measurement.outerWidth());
    measurement.remove();

    // Apply the measured name-column width to every section table.
    // Fixed table layout divides the remaining width equally
    // among source columns.
    sections.find('.config-parameters th:first-child')
        .css('width', parameterWidth);

    // Apply the latest expand/collapse request after any active animation ends.
    function collapseSection(section, action) {
        // Cancel handlers for older requests so only the latest action
        // is applied.
        // Keep Bootstrap's own event handlers.
        section.off('.configSearch');
        if (section.data('bs.collapse').transitioning) {
            section.one(
                'shown.bs.collapse.configSearch ' +
                    'hidden.bs.collapse.configSearch',
                function () {
                    // Remove pending completion handlers before starting
                    // the requested action.
                    section.off('.configSearch').collapse(action);
                });
        } else {
            section.collapse(action);
        }
    }

    // Expand or collapse sections visible under the current search filter.
    // Leave sections hidden by the filter unchanged.
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
        // Match names case-insensitively and ignore spaces around the query.
        var query = $.trim(this.value).toLowerCase();

        // Ignore short queries before searching starts. If an active query
        // becomes shorter than three characters, clear the filter and restore
        // section states.
        if (query.length < 3) {
            if (expandedSections === null) {
                return;
            }
            query = '';
        }

        // Save open section IDs once, before filtering first changes
        // their state.
        // Restore this original state when the search is cleared.
        if (query && expandedSections === null) {
            expandedSections = sections.filter('.in').map(function () {
                return this.id;
            }).get();
        }

        // Match parameter names only; keep source values and section totals
        // unchanged.
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

            // Hide both the section summary and its parameter table when no
            // rows match.
            var details = section.closest('tr');
            details.toggle(sectionMatches > 0);
            details.prev().toggle(sectionMatches > 0);

            // Open sections with matching rows while searching. When the filter
            // is cleared, restore each section to its saved state from before
            // the search.
            if (query && sectionMatches) {
                collapseSection(section, 'show');
            } else if (!query && expandedSections !== null) {
                collapseSection(section,
                    expandedSections.indexOf(this.id) !== -1 ? 'show' : 'hide');
            }
        });

        // Show the no-matches message only for an active query
        // with no matching rows.
        $('#config-search-empty').toggle(Boolean(query) && matches === 0);

        // Forget the saved section states after clearing the filter so the next
        // search captures a fresh starting state.
        if (!query) {
            expandedSections = null;
        }
    });
});
</script>)" << Endl;

        // Main page content below the styles and scripts.
        DIV_CLASS ("container") {
            // Page title above the configuration metadata.
            TAG (TH3) {
                out << "Config";
            }

            // Metadata table above the search and section controls.
            TABLE_CLASS ("table table-condensed config-summary") {
                // Rows for the last update and Dynamic source presence.
                TABLEBODY () {
                    // Last-update row, including any rejected delivery reason.
                    TABLER () {
                        // Label for the last accepted configuration update.
                        TABLED () {
                            out << "Last update";
                        }

                        // Update details beside the Last update label.
                        TABLED () {
                            // Time of the last accepted configuration update.
                            out << Data.LastUpdateTime.ToString() << ", ";

                            // Origin and status of that accepted update.
                            out << EncodeHtmlPcdata(
                                GetUpdateStatusName(Data.UpdateStatus));

                            if (Data.RejectedUpdateReason) {
                                // Highlight the last rejected delivery reason.
                                out << " <span class='config-update-rejected'>"
                                       "- rejected: "
                                    << EncodeHtmlPcdata(
                                           Data.RejectedUpdateReason)
                                    << "</span>";
                            }
                        }
                    }

                    // Presence of the current Dynamic configuration source.
                    TABLER () {
                        // Label for the Dynamic source presence indicator.
                        TABLED () {
                            out << "Dynamic config";
                        }

                        // Dynamic source indicator: present or absent.
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

            // Toolbar between the metadata and section list.
            out << "<div class='config-actions'>";

            // Search label wrapping the parameter-name input.
            out << "<label class='config-search' "
                   "for='config-parameter-search'>Search parameter: ";

            // Search input; filter parameter names after three characters.
            out << "<input id='config-parameter-search' type='search' "
                   "class='form-control input-sm' "
                   "placeholder='At least 3 characters'>";

            // End the search label after its input.
            out << "</label>";

            // Right-aligned controls for all visible configuration sections.
            out << "<div class='config-toggle-actions'>";

            // Collapse all visible section parameter tables.
            out << "<a href='#' id='config-collapse-all' role='button' "
                   "class='btn btn-default btn-sm'>Collapse all</a> ";

            // Expand all visible section parameter tables.
            out << "<a href='#' id='config-expand-all' role='button' "
                   "class='btn btn-default btn-sm'>Expand all</a>";

            // End the section controls.
            out << "</div>";

            // End the toolbar.
            out << "</div>";

            if (AnyOf(
                    Data.CurrentConfigPreparationDiagnostics.IgnoredPaths,
                    [](const auto& entry)
                    { return IsDeferredReason(entry.second); }))
            {
                // Restart explanation above the section list.
                out << "<p class='config-deferred-hint'>";

                // Deferred label at the start of the restart explanation.
                out << "<span class='config-dynamic-deferred'>Deferred</span> ";

                // Explain when the deferred changes take effect.
                out << "parameters cannot be updated at runtime and take "
                       "effect after restart.";

                // End the restart explanation.
                out << "</p>";
            }

            // Explain ignored values from either Dynamic source, including
            // retained startup history.
            const auto isStaticOnly = [](const auto& entry)
            {
                return entry.second == EConfigIgnoreReason::StaticOnly;
            };
            if (AnyOf(
                    Data.CurrentConfigPreparationDiagnostics.IgnoredPaths,
                    isStaticOnly) ||
                AnyOf(
                    Data.StartupDynamicConfigNormalizationDiagnostics
                        .IgnoredPaths,
                    isStaticOnly))
            {
                // Static-only explanation for either Dynamic source.
                out << "<p class='config-dynamic-ignored'>"
                    << "Static-only parameters detected in the dynamic config "
                    << "are ignored, even after a restart. Set them in the "
                    << "static configuration.</p>";
            }

            // Search result message, hidden until no parameters match.
            out << "<p id='config-search-empty' role='status' "
                   "style='display: none'>No matching parameters.</p>";

            // Section list with Dynamic and ICB counters beside each name.
            out << "<table class='table table-condensed' id='config-sections'>";

            // Column headings for the section list.
            out << "<thead>";

            // Header row above the section names and counters.
            out << "<tr>";

            // Unlabeled heading above section names and expand/collapse links.
            out << "<th></th>";

            // Heading above current Dynamic parameter counters.
            out << "<th>Dynamic</th>";

            // Heading above live ICB override counters.
            out << "<th>ICB</th>";

            // End the section-list header row.
            out << "</tr>";

            // End the section-list headings.
            out << "</thead>";

            // Section summary rows and their expandable parameter tables.
            out << "<tbody>";

            const auto* descriptor = NProto::TBlockstoreConfig::descriptor();

            for (int i = 0; i != descriptor->field_count(); ++i) {
                const auto* section = descriptor->field(i);
                const bool showIcb = section->name() == "StorageService";

                // Prepare this section's parameter comparison rows.
                TStringStream rows;
                TSectionCounts counts;
                const TRenderContext context{
                    .Controls = controls,
                    .Diagnostics = Data.CurrentConfigPreparationDiagnostics,
                    .StartupDynamicConfigNormalizationDiagnostics =
                        Data.StartupDynamicConfigNormalizationDiagnostics,
                    .CurrentDynamicSource = hasRuntimeUpdate
                                                ? RuntimeDynamicSource
                                                : StartupDynamicSource,
                    .ShowIcb = showIcb,
                };

                if (!RenderNestedMessage(
                        rows,
                        sources,
                        section,
                        section->name(),
                        context,
                        counts))
                {
                    continue;
                }

                const TString target = TStringBuilder()
                                       << "config-section-" << section->name();
                CountIgnoredParameters(
                    section->name(),
                    Data.CurrentConfigPreparationDiagnostics,
                    counts);

                // Section summary row; visible when its details are collapsed.
                out << "<tr>";

                // Section-name cell to the left of its counters.
                out << "<td>";

                // Section name linking to its expandable parameter table.
                out << "<a href='#" << target
                    << "' class='collapse-ref collapsed' "
                    << "data-toggle='collapse' "
                    << "data-target='#" << target
                    << "' aria-expanded='false' aria-controls='" << target
                    << "'>" << section->name() << "</a>";

                // End the section-name cell.
                out << "</td>";

                // Current Dynamic total, deferred count and ignored count.
                out << "<td>";

                // Current Dynamic parameter count, or a dash when zero.
                out << "<span class='"
                    << (counts.Dynamic ? "config-dynamic-total" : "config-zero")
                    << "'>";
                if (counts.Dynamic) {
                    out << counts.Dynamic;
                } else {
                    out << "&mdash;";
                }
                out << "</span>";

                if (counts.PendingRestart) {
                    // Separator before the deferred counter.
                    out << " / ";

                    // Count of changes requiring a restart.
                    out << "<span class='config-dynamic-deferred'>deferred: "
                        << counts.PendingRestart << "</span>";
                }

                if (counts.Ignored) {
                    // Separator before the ignored static-only counter.
                    out << " / ";

                    // Label for the ignored static-only parameter count.
                    out << "<span class='config-dynamic-ignored'>"
                           "ignored</span>";

                    // Ignored count in the current Dynamic source only.
                    out << ": " << counts.Ignored;
                }

                // End the Dynamic counters cell.
                out << "</td>";

                // ICB counter cell in the section summary.
                out << "<td>";

                // Live ICB override count, or a dash when zero.
                out << "<span class='"
                    << (counts.Icb ? "config-icb" : "config-zero") << "'>";
                if (counts.Icb) {
                    out << counts.Icb;
                } else {
                    out << "&mdash;";
                }
                out << "</span>";

                // End the ICB counter cell.
                out << "</td>";

                // End the section summary row.
                out << "</tr>";

                // Details row immediately below the section summary.
                out << "<tr class='config-details'>";

                // Details cell spanning the section name and both counters.
                out << "<td colspan='3'>";

                // Collapsible container targeted by the section-name link.
                out << "<div id='" << target
                    << "' class='collapse config-section'>";
                {
                    // Parameter comparison table inside this section's details.
                    TABLE_CLASS (
                        "table table-condensed table-bordered "
                        "config-parameters")
                    {
                        // Parameter-name and configuration-source headings.
                        TABLEHEAD () {
                            // Header row; ICB applies only to StorageService.
                            TABLER () {
                                for (const TStringBuf title:
                                     {
                                         "Parameter",
                                         "Static",
                                         "Dynamic<br>(startup)",
                                         "Dynamic<br>(runtime)",
                                         "ICB",
                                         "Effective",
                                     })
                                {
                                    if (title == "ICB" && !showIcb) {
                                        continue;
                                    }

                                    // Column caption in the parameter table.
                                    TABLEH () {
                                        out << title;
                                    }
                                }
                            }
                        }

                        // Parameter rows built above by the field renderers.
                        TABLEBODY () {
                            out << rows.Str();
                        }
                    }
                }

                // End the collapsible parameter-table container.
                out << "</div>";

                // End the details cell.
                out << "</td>";

                // End the section details row.
                out << "</tr>" << Endl;
            }

            // End the section summaries and details.
            out << "</tbody>";

            // End the section list table.
            out << "</table>";
        }
    }

    return out.Str();
}

}   // namespace NCloud::NBlockStore
