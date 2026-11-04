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
  const options = [
    { value: "", label: "Any matching span" },
    ...testPhaseOptions(spans, model.c),
  ];
  populateSelectOptions(testPhaseElement, options);
  populateSelectOptions(rowOrderElement, [
    { value: ROW_ORDERS.TREE, label: "Trace tree" },
    ...options.map(({ value, label }) => ({
      value: value || ROW_ORDERS.DURATION,
      label: `${label}, longest first`,
    })),
  ]);
}

function populateSelectOptions(element, options) {
  const previousValue = element.value;
  element.replaceChildren();
  options.forEach(({ value, label }) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    element.append(option);
  });
  if (options.some(({ value }) => value === previousValue)) {
    element.value = previousValue;
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
    updateOrderedRows();
    renderRows();
    return;
  }
  rowBudget = INITIAL_ROW_BUDGET;
  limits = new Map();
  visible = result.visible;
  filterStatus.textContent = `${result.matches.toLocaleString()} matching span${
    result.matches === 1 ? "" : "s"
  }`;
  updateOrderedRows();
  renderRows();
}

function currentRowOrder() {
  return rowOrderFromValue(rowOrderElement?.value);
}

function updateOrderedRows() {
  const order = currentRowOrder();
  orderedRows =
    order === ROW_ORDERS.TREE
      ? []
      : durationOrderedIndexes(traceIndex, order, currentFilters());
  expandElement.disabled = rowOrderIsFlat(order);
  collapseElement.disabled = rowOrderIsFlat(order);
}

function applyRowOrder() {
  rowBudget = INITIAL_ROW_BUDGET;
  limits = new Map();
  updateOrderedRows();
  renderRows();
}

function flattenRows() {
  const order = currentRowOrder();
  if (rowOrderIsFlat(order)) return flatOrderedRows(orderedRows, rowBudget);
  const ordered = order !== ROW_ORDERS.TREE;
  return flattenTraceRows({
    sourceSpans: spans,
    sourceChildren: children,
    sourceRoots: ordered ? orderedRows : roots,
    sourceExpanded: expanded,
    sourceLimits: limits,
    sourceVisible:
      ordered && visible ? new Set([...visible, ...orderedRows]) : visible,
    maximumRows: rowBudget,
  });
}

function toggleSpanGroup(index) {
  if (visible || !children[index].length) return;
  if (expanded.has(index)) {
    expanded.delete(index);
  } else {
    const flattened = flattenRows();
    if (currentRowOrder() !== ROW_ORDERS.TREE) {
      // Leave room for children even when the sorted group list fills the page.
      rowBudget = expandedGroupBudget(
        rowBudget,
        flattened.spanRows,
        limits.get(index) ?? Math.min(PAGE_SIZE, children[index].length),
      );
    }
    if (!limits.has(index)) {
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
