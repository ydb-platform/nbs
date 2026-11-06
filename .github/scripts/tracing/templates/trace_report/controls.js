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
  matchingSpans = result.matching;
  if (result.visible === null) {
    visible = null;
    resetDefaults();
    updateOrderedRows();
    renderRows();
    return;
  }
  rowBudget = INITIAL_ROW_BUDGET;
  limits = new Map();
  visible = result.visible;
  updateOrderedRows();
  renderRows();
}

function currentRowOrder() {
  return rowOrderFromValue(rowOrderElement?.value);
}

function treeFilterIsActive() {
  return Boolean(visible) && currentRowOrder() === ROW_ORDERS.TREE;
}

function updateOrderedRows() {
  const order = currentRowOrder();
  orderedRows =
    order === ROW_ORDERS.TREE
      ? []
      : durationOrderedIndexes(traceIndex, order, currentFilters());
  const matches = order === ROW_ORDERS.TREE ? matchingSpans?.size : orderedRows.length;
  filterStatus.textContent = matches === undefined
    ? ""
    : `${matches.toLocaleString()} matching span${matches === 1 ? "" : "s"}`;
  orderedVisible = visible;
  if (order !== ROW_ORDERS.TREE && !rowOrderIsFlat(order) && visible) {
    orderedVisible = new Set([...visible, ...orderedRows]);
    // A directly matching interval can expose its contents. Context ancestors
    // must not expose unrelated branches that failed the duration selection.
    const matchingGroups = [...matchingSpans].filter((index) =>
      testPhaseDefinition(traceIndex.scope(index))?.expandable,
    );
    traceIndex.addDescendants(orderedVisible, matchingGroups);
    // Start with matching paths open; subsequent expansion and collapse are
    // controlled by the user, including when a duration filter is active.
    visible.forEach((index) => {
      if (children[index].some((child) => visible.has(child))) expanded.add(index);
    });
  }
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
    sourceVisible: ordered ? orderedVisible : visible,
    autoExpandVisible: !ordered,
    maximumRows: rowBudget,
  });
}

function toggleSpanGroup(index) {
  if (treeFilterIsActive() || !children[index].length) return;
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

function expandAllGroups() {
  spans.forEach((_span, index) => {
    if (children[index].length) expanded.add(index);
  });
  renderRows();
}

function collapseAllGroups() {
  expanded.clear();
  renderRows();
}
