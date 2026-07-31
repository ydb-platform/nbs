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

function isPressed(element) {
  return element.getAttribute("aria-pressed") === "true";
}

function selectedTestSizes() {
  return new Set(
    [...testSizeElements]
      .filter((element) => isPressed(element))
      .map((element) => element.dataset.testSize),
  );
}

function filterControlActivity(
  {
    query = "",
    failedOnly = false,
    topTestsOnly = false,
    minimumDurationValue = "",
    minimumDurationBadInput = false,
    testPhaseValue = "",
    testSizes = new Set(),
  } = {},
) {
  const hasQuery = String(query ?? "").length > 0;
  const hasMinimumDuration =
    String(minimumDurationValue ?? "").length > 0 ||
    Boolean(minimumDurationBadInput);
  const hasTestPhase = String(testPhaseValue ?? "").length > 0;
  const hasTestSizes = Boolean(testSizes?.size || testSizes?.length);
  return {
    query: hasQuery,
    minimumDuration: hasMinimumDuration,
    any: Boolean(
      hasQuery ||
        failedOnly ||
        topTestsOnly ||
        hasMinimumDuration ||
        hasTestPhase ||
        hasTestSizes
    ),
  };
}

function clearedFilterControlState(state = {}) {
  return {
    ...state,
    query: "",
    failedOnly: false,
    topTestsOnly: false,
    minimumDurationValue: "",
    minimumDurationBadInput: false,
    testPhaseValue: "",
    testSizes: new Set(),
  };
}

function currentFilterControlState() {
  return {
    query: filterElement.value,
    failedOnly: isPressed(failedOnlyElement),
    topTestsOnly: isPressed(topTestsOnlyElement),
    minimumDurationValue: minimumDurationElement.value,
    minimumDurationBadInput: Boolean(minimumDurationElement.validity?.badInput),
    testPhaseValue: testPhaseElement.value,
    testSizes: selectedTestSizes(),
  };
}

function updateFilterClearControls() {
  const activity = filterControlActivity(currentFilterControlState());
  clearFilterElement.hidden = !activity.query;
  clearFilterElement.disabled = filterElement.disabled || !activity.query;
  clearMinimumDurationElement.hidden = !activity.minimumDuration;
  clearMinimumDurationElement.disabled =
    minimumDurationElement.disabled || !activity.minimumDuration;
  clearFiltersElement.disabled = filterElement.disabled || !activity.any;
}

function currentFilters() {
  return {
    query: filterElement.value,
    failedOnly: isPressed(failedOnlyElement),
    topTestsOnly: isPressed(topTestsOnlyElement),
    minimumDurationNs: parseMinimumDurationNs(minimumDurationElement.value),
    testSizes: selectedTestSizes(),
    testPhase: testPhaseElement.value,
    scopes: model.c,
  };
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

function clearAllFilters() {
  const cleared = clearedFilterControlState(currentFilterControlState());
  filterElement.value = cleared.query;
  failedOnlyElement.setAttribute("aria-pressed", String(cleared.failedOnly));
  topTestsOnlyElement.setAttribute("aria-pressed", String(cleared.topTestsOnly));
  minimumDurationElement.value = cleared.minimumDurationValue;
  testPhaseElement.value = cleared.testPhaseValue;
  testSizeElements.forEach((element) => {
    element.setAttribute("aria-pressed", "false");
  });
  updateFilterClearControls();
  applyFilterImmediately();
  filterElement.focus();
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

function toggleFilter(element) {
  element.setAttribute("aria-pressed", String(!isPressed(element)));
  applyFilterImmediately();
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
