function initialize(decoded) {
  model = decoded;
  spans = model.s;
  searchCache = [];
  traceIndex = new TraceIndex(spans, model.c, searchCache);
  ({ children, roots } = traceIndex);
  resetDefaults();
  populateTestPhaseOptions();
  [
    ...filterForm.elements,
    rowLoadSizeElement,
    timelineModeElement,
    document.getElementById("expand"),
    document.getElementById("collapse"),
  ].forEach((element) => {
    element.disabled = false;
  });
  updateFilterClearControls();
  renderRows();
}

function scheduleFilter() {
  updateFilterClearControls();
  clearTimeout(filterTimer);
  filterTimer = setTimeout(applyFilter, 120);
}

function startTraceReport() {
  traceElement = document.getElementById("trace");
  traceHeadElement = document.getElementById("trace-head");
  columnResizerElement = document.getElementById("column-resizer");
  rowsElement = document.getElementById("rows");
  filterForm = document.getElementById("filters");
  filterElement = document.getElementById("filter");
  clearFilterElement = document.getElementById("clear-filter");
  filterStatus = document.getElementById("filter-status");
  minimumDurationElement = document.getElementById("minimum-duration");
  clearMinimumDurationElement = document.getElementById(
    "clear-minimum-duration",
  );
  testPhaseElement = document.getElementById("test-phase");
  clearFiltersElement = document.getElementById("clear-filters");
  rowLoadSizeElement = document.getElementById("row-load-size");
  timelineModeElement = document.getElementById("timeline-mode");
  rowLoader = document.getElementById("row-loader");
  rowLoadButton = document.getElementById("load-rows");
  rowStatus = document.getElementById("row-status");
  initializeColumnResizer();

  filterForm.addEventListener("input", ({ target }) => {
    if (target === filterElement || target === minimumDurationElement) {
      scheduleFilter();
    }
  });
  filterForm.addEventListener("submit", (event) => event.preventDefault());
  filterForm.addEventListener("change", applyFilterImmediately);
  filterForm.addEventListener("reset", () => {
    setTimeout(() => {
      applyFilterImmediately();
      filterElement.focus();
    });
  });
  clearFilterElement.addEventListener("click", clearTextFilter);
  clearMinimumDurationElement.addEventListener(
    "click",
    clearMinimumDurationFilter,
  );
  rowLoadSizeElement.addEventListener("change", renderRows);
  timelineModeElement.addEventListener("change", renderRows);
  rowsElement.addEventListener("click", handleRowsClick);
  rowLoadButton.addEventListener("click", () => {
    rowBudget = nextRowLimit(rowBudget, selectedLoadSize());
    renderRows();
  });
  document.getElementById("expand").addEventListener("click", () => {
    spans.forEach((_span, index) => {
      if (children[index].length) expanded.add(index);
    });
    renderRows();
  });
  document.getElementById("collapse").addEventListener("click", () => {
    expanded.clear();
    renderRows();
  });
  decodeModel()
    .then(initialize)
    .catch((error) => {
      const loading = document.getElementById("loading");
      loading.textContent = `Unable to load trace: ${error.message}`;
      loading.style.color = "var(--bad)";
    });
}

const traceReportApi = {
  FIELDS: {
    ID,
    PARENT,
    NAME,
    START,
    DURATION,
    ATTRS,
    EVENTS,
    STATUS,
    STATUS_MESSAGE,
    RESOURCE,
    SCOPE,
    TRACE,
    ORPHAN_PARENT,
  },
  PAGE_SIZE,
  LOAD_SIZE_OPTIONS,
  INITIAL_ROW_BUDGET,
  NAME_COLUMN_STORAGE_KEY,
  TIMELINE_MODES,
  LOCAL_TIMELINE_ROOT_SCOPES,
  COLLAPSED_SCOPES,
  TEST_SIZES,
  TEST_PHASE_DEFINITIONS,
  TraceIndex,
  formatDuration,
  childCountLabel,
  directChildCountLabel,
  isCriticalPathTest,
  criticalPathBadge,
  longestTestRank,
  parseMinimumDurationNs,
  encodeTestPhaseSelection,
  parseTestPhaseSelection,
  testPhaseOptions,
  timelineBarClass,
  timelineModeFromValue,
  timelineBarGeometry,
  buildHierarchy,
  defaultExpanded,
  spanSearchText,
  normalizeFilterSpec,
  matchingVisibility,
  filterVisibility,
  flattenTraceRows,
  loadSizeFromValue,
  nextRowLimit,
  groupLoadPlan,
  initialGroupLimit,
  clampNameColumnWidth,
  readStoredNameColumnWidth,
  writeStoredNameColumnWidth,
  inlineDetailRows,
  safeArtifactPath,
  artifactLinkForAttribute,
  linkableHttpUrl,
  nextSelectedSpan,
};

if (typeof module === "object" && module.exports) {
  module.exports = traceReportApi;
} else {
  startTraceReport();
}
