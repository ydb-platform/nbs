const TRACE_VIEW_QUERY_KEYS = Object.freeze([
  "query",
  "failed",
  "top-tests",
  "minimum-duration",
  "test-size",
  "phase",
  "order",
  "timeline",
  "rows",
]);

function testPhaseQueryValue(value) {
  const selection = normalizedTestPhase(value);
  if (!selection) return "";
  return selection.name === null
    ? selection.scope
    : `${selection.scope}/${selection.name}`;
}

function testPhaseFromQueryValue(value) {
  if (!value) return "";
  const separator = value.indexOf("/");
  const scope = separator < 0 ? value : value.slice(0, separator);
  const name = separator < 0 ? null : value.slice(separator + 1);
  const encoded = encodeTestPhaseSelection(scope, name);
  return parseTestPhaseSelection(encoded) ? encoded : "";
}

function readTraceView(search) {
  const params = new URLSearchParams(search);
  const flag = (key) =>
    params.has(key) &&
    ["", "1", "true"].includes(params.get(key));
  const minimum = parseMinimumDurationNs(params.get("minimum-duration") || "");
  const order = params.get("order") || ROW_ORDERS.TREE;
  return {
    query: params.get("query") || "",
    failedOnly: flag("failed"),
    topTestsOnly: flag("top-tests"),
    minimumDurationNs: Number.isFinite(minimum) ? minimum : null,
    testSizes: new Set(
      params.getAll("test-size").filter((size) => TEST_SIZES.includes(size)),
    ),
    testPhase: testPhaseFromQueryValue(params.get("phase")),
    order: rowOrderFromValue(testPhaseFromQueryValue(order) || order),
    timelineMode: timelineModeFromValue(params.get("timeline")),
    rowLoadSize: loadSizeFromValue(params.get("rows")),
  };
}

function traceViewUrl(href, view) {
  const url = new URL(href);
  const params = url.searchParams;
  TRACE_VIEW_QUERY_KEYS.forEach((key) => params.delete(key));
  const filters = normalizeFilterSpec(view);
  if (view.query) params.set("query", view.query);
  if (filters.failedOnly) params.set("failed", "1");
  if (filters.topTestsOnly) params.set("top-tests", "1");
  if (filters.minimumDurationNs !== null) {
    params.set("minimum-duration", String(filters.minimumDurationNs / 1e9));
  }
  TEST_SIZES.filter((size) => filters.testSizes.has(size)).forEach((size) => {
    params.append("test-size", size);
  });
  const phase = testPhaseQueryValue(filters.testPhase);
  if (phase) params.set("phase", phase);
  const order = rowOrderFromValue(view.order);
  if (order !== ROW_ORDERS.TREE) {
    params.set("order", testPhaseQueryValue(order) || order);
  }
  const timeline = timelineModeFromValue(view.timelineMode);
  if (timeline !== TIMELINE_MODES.GLOBAL) params.set("timeline", timeline);
  const rows = loadSizeFromValue(view.rowLoadSize);
  if (rows !== DEFAULT_LOAD_SIZE) params.set("rows", String(rows));
  return url.href;
}

function restoreTraceViewFromUrl() {
  const view = readTraceView(window.location.search);
  filterElement.value = view.query;
  minimumDurationElement.value =
    view.minimumDurationNs === null
      ? ""
      : String(view.minimumDurationNs / 1e9);
  filterForm.elements.namedItem("failed").checked = view.failedOnly;
  filterForm.elements.namedItem("top-tests").checked = view.topTestsOnly;
  [...filterForm.elements]
    .filter(({ name }) => name === "test-size")
    .forEach((element) => {
      element.checked = view.testSizes.has(element.value);
    });
  for (const [element, value, fallback] of [
    [testPhaseElement, view.testPhase, ""],
    [rowOrderElement, view.order, ROW_ORDERS.TREE],
    [timelineModeElement, view.timelineMode, TIMELINE_MODES.GLOBAL],
    [rowLoadSizeElement, String(view.rowLoadSize), String(DEFAULT_LOAD_SIZE)],
  ]) {
    element.value = [...element.options].some((option) => option.value === value)
      ? value
      : fallback;
  }
}

function syncTraceViewUrl() {
  const href = traceViewUrl(window.location.href, {
    ...currentFilters(),
    order: currentRowOrder(),
    timelineMode: timelineModeElement.value,
    rowLoadSize: selectedLoadSize(),
  });
  if (href !== lastViewUrl) {
    shareStatus.textContent = "";
    shareLinkControl.hidden = true;
    lastViewUrl = href;
  }
  if (href !== window.location.href) {
    try {
      window.history.replaceState(window.history.state, "", href);
    } catch (_error) {
      // Some browsers restrict history changes for local reports. The copy
      // button can still provide the URL with the selected query parameters.
    }
  }
  return href;
}

async function copyTraceViewLink() {
  if (filterTimer !== undefined) applyFilterImmediately();
  const href = syncTraceViewUrl();
  try {
    await navigator.clipboard.writeText(href);
    shareStatus.textContent = "Link copied.";
  } catch (_error) {
    shareLinkControl.hidden = false;
    shareLinkElement.value = href;
    shareLinkElement.focus();
    shareLinkElement.select();
    shareStatus.textContent = "Copy the selected link.";
  }
}
