async function decodeModel() {
  if (!("DecompressionStream" in window)) {
    throw new Error(
      "This report requires a browser with DecompressionStream support.",
    );
  }
  const encoded = document.getElementById("trace-data").textContent.trim();
  const binary = atob(encoded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  const stream = new Blob([bytes])
    .stream()
    .pipeThrough(new DecompressionStream("gzip"));
  return JSON.parse(await new Response(stream).text());
}

function resetDefaults() {
  expanded = defaultExpanded(spans, children, model.c);
  limits = new Map();
  rowBudget = INITIAL_ROW_BUDGET;
}

function currentFilters() {
  const values = new FormData(filterForm);
  return {
    query: values.get("query") || "",
    failedOnly: values.has("failed"),
    topTestsOnly: values.has("top-tests"),
    minimumDurationNs: parseMinimumDurationNs(
      values.get("minimum-duration") || "",
    ),
    testSizes: new Set(values.getAll("test-size")),
    testPhase: values.get("phase") || "",
    scopes: model.c,
  };
}

function updateFilterClearControls() {
  const filters = normalizeFilterSpec(currentFilters());
  const hasQuery = filterElement.value.length > 0;
  const hasMinimum =
    minimumDurationElement.value.length > 0 ||
    Boolean(minimumDurationElement.validity?.badInput);
  clearFilterElement.hidden = !hasQuery;
  clearFilterElement.disabled = filterElement.disabled || !hasQuery;
  clearMinimumDurationElement.hidden = !hasMinimum;
  clearMinimumDurationElement.disabled =
    minimumDurationElement.disabled || !hasMinimum;
  clearFiltersElement.disabled =
    filterElement.disabled || !(filters.active || hasQuery || hasMinimum);
}

function populateTestPhaseOptions() {
  const previousValue = testPhaseElement.value;
  testPhaseElement.replaceChildren();
  const anyPhase = document.createElement("option");
  anyPhase.value = "";
  anyPhase.textContent = "Any matching span";
  testPhaseElement.append(anyPhase);
  testPhaseOptions(spans, model.c).forEach(({ value, label }) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    testPhaseElement.append(option);
  });
  if ([...testPhaseElement.options].some(({ value }) => value === previousValue)) {
    testPhaseElement.value = previousValue;
  }
}

function applyFilterImmediately() {
  clearTimeout(filterTimer);
  filterTimer = undefined;
  applyFilter();
}

function clearTextFilter() {
  filterElement.value = "";
  updateFilterClearControls();
  applyFilterImmediately();
  filterElement.focus();
}

function clearMinimumDurationFilter() {
  minimumDurationElement.value = "";
  updateFilterClearControls();
  applyFilterImmediately();
  minimumDurationElement.focus();
}

function applyFilter() {
  updateFilterClearControls();
  const result = filterVisibility(
    spans,
    currentFilters(),
    searchCache,
    traceIndex,
  );
  if (result.visible === null) {
    visible = null;
    filterStatus.textContent = "";
    resetDefaults();
    renderRows();
    return;
  }
  rowBudget = INITIAL_ROW_BUDGET;
  limits = new Map();
  visible = result.visible;
  filterStatus.textContent = `${result.matches.toLocaleString()} matching span${
    result.matches === 1 ? "" : "s"
  }`;
  renderRows();
}

function flattenRows() {
  return flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: children,
    sourceRoots: roots,
    sourceExpanded: expanded,
    sourceLimits: limits,
    sourceVisible: visible,
    maximumRows: rowBudget,
  });
}

function toggleSpanGroup(index) {
  if (visible || !children[index].length) return;
  if (expanded.has(index)) {
    expanded.delete(index);
  } else {
    if (!limits.has(index)) {
      const flattened = flattenRows();
      limits.set(
        index,
        initialGroupLimit(
          rowBudget,
          flattened.spanRows,
          children[index].length,
        ),
      );
    }
    expanded.add(index);
  }
  renderRows();
}
