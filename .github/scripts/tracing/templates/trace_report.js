"use strict";

// Static trace report browser model.
const ID = 0;
const PARENT = 1;
const NAME = 2;
const START = 3;
const DURATION = 4;
const ATTRS = 5;
const EVENTS = 6;
const STATUS = 7;
const STATUS_MESSAGE = 8;
const RESOURCE = 9;
const SCOPE = 10;
const TRACE = 11;
const ORPHAN_PARENT = 12;
const PAGE_SIZE = 200;
const LOAD_SIZE_OPTIONS = Object.freeze([200, 1000, 5000]);
const INITIAL_ROW_BUDGET = PAGE_SIZE;
const NAME_COLUMN_STORAGE_KEY = "trace-report.name-column-width";
const MIN_NAME_COLUMN_WIDTH_PX = 240;
const MIN_OTHER_COLUMNS_WIDTH_PX = 400;
const NAME_COLUMN_KEYBOARD_STEP_PX = 24;
const MIN_TIMELINE_BAR_PERCENT = 0.15;
const TIMELINE_MODES = Object.freeze({
  GLOBAL: "global",
  LOCAL: "local",
});
const LOCAL_TIMELINE_ROOT_SCOPES = new Set([
  "ya.test.worker",
  "ya.test.node",
  "ya.chunk",
]);
const COLLAPSED_SCOPES = new Set([
  "ya.build",
  "ya.chunk",
  "ya.test.operations",
  "ya.test.node",
  "ya.test.worker",
]);
const TEST_SIZES = Object.freeze(["small", "medium", "large"]);
const TEST_PHASE_DEFINITIONS = Object.freeze([
  Object.freeze({
    scope: "ya.test.stage",
    attribute: "ya.test.stage.name",
    label: "Test stage",
    ownerScopes: Object.freeze(["ya.chunk"]),
  }),
  Object.freeze({
    scope: "ya.test.worker.phase",
    attribute: "ya.test.worker.phase",
    label: "Worker phase",
    ownerScopes: Object.freeze(["ya.test.worker", "ya.test.node"]),
  }),
]);
const TEST_PHASE_NAME_LABELS = Object.freeze({
  exec_cmd: "exec command",
  post_cmd: "post command",
  node_result: "node result",
});
const TEST_WORKER_PHASE_BAR_CLASSES = Object.freeze({
  setup: "bar-worker-setup",
  exec_cmd: "bar-worker-exec-cmd",
  post_cmd: "bar-worker-post-cmd",
  node_result: "bar-worker-node-result",
  finalize: "bar-worker-finalize",
});
const TEST_STAGE_BAR_CLASSES = Object.freeze({
  prepare_recipes: "bar-stage-prepare-recipes",
  wrapper_execution: "bar-stage-wrapper-execution",
  stop_recipes: "bar-stage-stop-recipes",
});

let rowsElement;
let filterElement;
let clearFilterElement;
let filterStatus;
let failedOnlyElement;
let topTestsOnlyElement;
let minimumDurationElement;
let clearMinimumDurationElement;
let testPhaseElement;
let testSizeElements;
let clearFiltersElement;
let rowLoadSizeElement;
let timelineModeElement;
let rowLoader;
let rowLoadButton;
let rowStatus;
let traceElement;
let traceHeadElement;
let columnResizerElement;
let columnResizeState = null;
let model;
let spans;
let children;
let roots;
let expanded;
let limits;
let visible;
let selected = null;
let searchCache = [];
let rowBudget = INITIAL_ROW_BUDGET;
let filterTimer;

function clampNameColumnWidth(width, containerWidth) {
  const availableWidth = Number(containerWidth);
  const maximum = Math.max(
    MIN_NAME_COLUMN_WIDTH_PX,
    (Number.isFinite(availableWidth) ? Math.floor(availableWidth) : 0) -
      MIN_OTHER_COLUMNS_WIDTH_PX,
  );
  const requestedWidth = Number(width);
  if (!Number.isFinite(requestedWidth)) return MIN_NAME_COLUMN_WIDTH_PX;
  return Math.round(
    Math.min(maximum, Math.max(MIN_NAME_COLUMN_WIDTH_PX, requestedWidth)),
  );
}

function readStoredNameColumnWidth(storage) {
  try {
    const width = Number(storage?.getItem(NAME_COLUMN_STORAGE_KEY));
    return Number.isFinite(width) && width > 0 ? width : null;
  } catch (_error) {
    return null;
  }
}

function writeStoredNameColumnWidth(storage, width) {
  try {
    if (!storage || !Number.isFinite(width)) return false;
    storage.setItem(NAME_COLUMN_STORAGE_KEY, String(Math.round(width)));
    return true;
  } catch (_error) {
    return false;
  }
}

function loadSizeFromValue(value) {
  const parsed = Number(value);
  return LOAD_SIZE_OPTIONS.includes(parsed) ? parsed : LOAD_SIZE_OPTIONS[0];
}

function selectedLoadSize() {
  return loadSizeFromValue(rowLoadSizeElement?.value || "");
}

function nextRowLimit(current, increment, maximum = Number.POSITIVE_INFINITY) {
  const currentValue = Math.max(0, Number(current) || 0);
  const incrementValue = Math.max(0, Number(increment) || 0);
  return Math.min(currentValue + incrementValue, maximum);
}

function groupLoadPlan(current, total, requested) {
  const remaining = Math.max(0, total - current);
  const count = Math.min(loadSizeFromValue(requested), remaining);
  return {
    count,
    nextLimit: nextRowLimit(current, count, total),
    remaining,
  };
}

function initialGroupLimit(rowLimit, renderedRows, childCount) {
  const availableRows = Math.max(0, rowLimit - renderedRows);
  return Math.min(PAGE_SIZE, childCount, availableRows);
}

function formatDuration(durationNs) {
  const duration = durationNs / 1e9;
  if (duration < 0.001) return `${(duration * 1e6).toFixed(0)} µs`;
  if (duration < 1) return `${(duration * 1e3).toFixed(1)} ms`;
  if (duration < 60) return `${duration.toFixed(3)} s`;
  const minutes = Math.floor(duration / 60);
  const seconds = duration - minutes * 60;
  if (minutes < 60) return `${minutes}m ${seconds.toFixed(1)}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes - hours * 60}m ${seconds.toFixed(0)}s`;
}

function childCountLabel(count, scope) {
  let noun = "item";
  if (scope === "ya.chunk") noun = "test";
  else if (scope === "ya.build") noun = "operation";
  else if (scope === "ya.test.operations") noun = "chunk";
  return `${count.toLocaleString()} ${noun}${count === 1 ? "" : "s"}`;
}

function directChildCountLabel(index, sourceSpans, sourceChildren, scopes) {
  const scope = scopes[sourceSpans[index][SCOPE]];
  const directChildren = sourceChildren[index] || [];
  let count = directChildren.length;
  if (scope === "ya.chunk") {
    count = directChildren.filter(
      (child) => scopes[sourceSpans[child][SCOPE]] === "ya.test",
    ).length;
  } else if (scope === "ya.test.operations") {
    const chunkCount = directChildren.filter(
      (child) =>
        scopes[sourceSpans[child][SCOPE]] === "ya.chunk" ||
        scopes[sourceSpans[child][SCOPE]] === "ya.test.worker",
    ).length;
    const operationCount = directChildren.filter(
      (child) => scopes[sourceSpans[child][SCOPE]] === "ya.test.node",
    ).length;
    const chunks = childCountLabel(chunkCount, scope);
    if (!operationCount) return chunks;
    return `${chunks} · ${operationCount.toLocaleString()} other operation${
      operationCount === 1 ? "" : "s"
    }`;
  } else if (scope === "ya.test.worker") {
    const chunkChildren = directChildren.filter(
      (child) => scopes[sourceSpans[child][SCOPE]] === "ya.chunk",
    );
    const testCount = chunkChildren.reduce(
      (total, chunk) =>
        total +
        (sourceChildren[chunk] || []).filter(
          (child) => scopes[sourceSpans[child][SCOPE]] === "ya.test",
        ).length,
      0,
    );
    const phaseCount = directChildren.filter(
      (child) => scopes[sourceSpans[child][SCOPE]] === "ya.test.worker.phase",
    ).length;
    const tests = childCountLabel(testCount, "ya.chunk");
    if (!phaseCount) return tests;
    return `${tests} · ${phaseCount.toLocaleString()} phase${
      phaseCount === 1 ? "" : "s"
    }`;
  } else if (scope === "ya.test.node") {
    const phaseCount = directChildren.filter(
      (child) => scopes[sourceSpans[child][SCOPE]] === "ya.test.worker.phase",
    ).length;
    if (phaseCount) {
      return `${phaseCount.toLocaleString()} phase${
        phaseCount === 1 ? "" : "s"
      }`;
    }
  }
  return childCountLabel(count, scope);
}

function isCriticalPathTest(span) {
  return span[ATTRS]["ya.test.critical_path"] === true;
}

function longestTestRank(span) {
  const rank = Number(span[ATTRS]["ya.test.duration.rank"]);
  return Number.isInteger(rank) && rank >= 1 && rank <= 10 ? rank : null;
}

function parseMinimumDurationNs(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return null;
  return Math.round(seconds * 1e9);
}

function testPhaseDefinition(scope) {
  return TEST_PHASE_DEFINITIONS.find(
    (definition) => definition.scope === scope,
  );
}

function encodeTestPhaseSelection(scope, name = null) {
  return JSON.stringify([scope, name]);
}

function parseTestPhaseSelection(value) {
  if (typeof value !== "string" || !value) return null;
  try {
    const decoded = JSON.parse(value);
    if (
      !Array.isArray(decoded) ||
      decoded.length !== 2 ||
      typeof decoded[0] !== "string" ||
      (decoded[1] !== null &&
        (typeof decoded[1] !== "string" || !decoded[1])) ||
      !testPhaseDefinition(decoded[0])
    ) {
      return null;
    }
    return { scope: decoded[0], name: decoded[1] };
  } catch (_error) {
    return null;
  }
}

function humanizeTestPhaseName(name) {
  return (
    TEST_PHASE_NAME_LABELS[name] ||
    name.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim()
  );
}

function testPhaseOptions(sourceSpans, scopes) {
  const phases = new Map();
  const availableScopes = new Set();
  sourceSpans.forEach((span) => {
    const scope = scopes[span[SCOPE]];
    const definition = testPhaseDefinition(scope);
    if (!definition) return;
    availableScopes.add(scope);
    const name = span[ATTRS][definition.attribute];
    if (typeof name !== "string" || !name) return;
    const value = encodeTestPhaseSelection(scope, name);
    phases.set(value, {
      value,
      scope,
      name,
      label: `${definition.label}: ${humanizeTestPhaseName(name)}`,
    });
  });
  const scopeOptions = TEST_PHASE_DEFINITIONS.filter(({ scope }) =>
    availableScopes.has(scope),
  ).map(({ scope, label }) => ({
    value: encodeTestPhaseSelection(scope),
    scope,
    name: null,
    label: `Any ${label.toLowerCase()}`,
  }));
  const exactOptions = [...phases.values()].sort(
    (left, right) =>
      TEST_PHASE_DEFINITIONS.findIndex(
        (definition) => definition.scope === left.scope,
      ) -
        TEST_PHASE_DEFINITIONS.findIndex(
          (definition) => definition.scope === right.scope,
        ) || left.label.localeCompare(right.label),
  );
  return [...scopeOptions, ...exactOptions];
}

function spanMatchesTestPhase(span, scopes, selection) {
  const scope = scopes[span[SCOPE]];
  if (scope !== selection.scope) return false;
  if (selection.name === null) return true;
  const definition = testPhaseDefinition(scope);
  return Boolean(
    definition && span[ATTRS][definition.attribute] === selection.name,
  );
}

function spanOrAncestorMatches(index, sourceSpans, predicate) {
  let current = index;
  const seen = new Set();
  while (
    current >= 0 &&
    current < sourceSpans.length &&
    !seen.has(current)
  ) {
    seen.add(current);
    if (predicate(sourceSpans[current], current)) return true;
    current = sourceSpans[current][PARENT];
  }
  return false;
}

function testPhaseOrOwnerMatches(
  index,
  sourceSpans,
  scopes,
  selection,
  predicate,
) {
  const definition = testPhaseDefinition(selection.scope);
  if (!definition) return false;
  if (predicate(sourceSpans[index], index)) return true;

  let current = sourceSpans[index][PARENT];
  const seen = new Set([index]);
  while (
    current >= 0 &&
    current < sourceSpans.length &&
    !seen.has(current)
  ) {
    seen.add(current);
    const candidate = sourceSpans[current];
    if (definition.ownerScopes.includes(scopes[candidate[SCOPE]])) {
      return predicate(candidate, current);
    }
    current = candidate[PARENT];
  }
  return false;
}

function timelineBarClass(scope, attributes) {
  const values =
    attributes && typeof attributes === "object" ? attributes : {};
  if (scope === "ya.test.worker.phase") {
    const phase = values["ya.test.worker.phase"];
    return Object.prototype.hasOwnProperty.call(
      TEST_WORKER_PHASE_BAR_CLASSES,
      phase,
    )
      ? TEST_WORKER_PHASE_BAR_CLASSES[phase]
      : "bar-default";
  }
  if (scope === "ya.test.stage") {
    const stage = values["ya.test.stage.name"];
    return Object.prototype.hasOwnProperty.call(TEST_STAGE_BAR_CLASSES, stage)
      ? TEST_STAGE_BAR_CLASSES[stage]
      : "bar-stage-other";
  }
  return "bar-default";
}

function timelineModeFromValue(value) {
  return value === TIMELINE_MODES.GLOBAL
    ? TIMELINE_MODES.GLOBAL
    : TIMELINE_MODES.LOCAL;
}

function globalTimelineBarGeometry(span, traceDuration) {
  const duration = Number(traceDuration);
  if (!Number.isFinite(duration) || duration <= 0) {
    return { left: 0, width: 100, relativeTo: -1 };
  }
  return boundedTimelineBarGeometry(span, 0, duration, -1);
}

function boundedTimelineBarGeometry(
  span,
  timelineStart,
  timelineDuration,
  relativeTo,
) {
  const spanStart = Number(span[START]);
  const spanDuration = Math.max(0, Number(span[DURATION]));
  if (!Number.isFinite(spanStart) || !Number.isFinite(spanDuration)) {
    return { left: 0, width: 0, relativeTo };
  }
  const spanEnd = spanStart + spanDuration;
  const timelineEnd = timelineStart + timelineDuration;
  const clippedStart = Math.max(timelineStart, spanStart);
  const clippedEnd = Math.min(timelineEnd, spanEnd);
  const clippedDuration = Math.max(0, clippedEnd - clippedStart);
  const instantWithinTimeline =
    spanDuration === 0 &&
    spanStart >= timelineStart &&
    spanStart <= timelineEnd;
  const exactLeft = Math.min(
    100,
    Math.max(0, (100 * (clippedStart - timelineStart)) / timelineDuration),
  );
  if (!clippedDuration && !instantWithinTimeline) {
    return { left: exactLeft, width: 0, relativeTo };
  }
  const width = Math.min(
    100,
    Math.max(
      MIN_TIMELINE_BAR_PERCENT,
      (100 * clippedDuration) / timelineDuration,
    ),
  );
  return {
    left: Math.min(exactLeft, 100 - width),
    width,
    relativeTo,
  };
}

function nearestLocalTimelineRoot(index, sourceSpans, scopes) {
  const seen = new Set([index]);
  let current = sourceSpans[index]?.[PARENT];
  while (
    Number.isInteger(current) &&
    current >= 0 &&
    current < sourceSpans.length &&
    !seen.has(current)
  ) {
    if (LOCAL_TIMELINE_ROOT_SCOPES.has(scopes[sourceSpans[current][SCOPE]])) {
      return current;
    }
    seen.add(current);
    current = sourceSpans[current][PARENT];
  }
  return -1;
}

function timelineBarGeometry(
  index,
  sourceSpans,
  scopes,
  traceDuration,
  timelineMode = TIMELINE_MODES.LOCAL,
) {
  const span = sourceSpans[index];
  if (timelineModeFromValue(timelineMode) === TIMELINE_MODES.GLOBAL) {
    return globalTimelineBarGeometry(span, traceDuration);
  }

  const scope = scopes[span[SCOPE]];
  if (LOCAL_TIMELINE_ROOT_SCOPES.has(scope)) {
    const localDuration = Number(span[DURATION]);
    if (!Number.isFinite(localDuration) || localDuration <= 0) {
      return globalTimelineBarGeometry(span, traceDuration);
    }
    return { left: 0, width: 100, relativeTo: index };
  }

  const timelineRootIndex = nearestLocalTimelineRoot(index, sourceSpans, scopes);
  if (timelineRootIndex < 0) {
    return globalTimelineBarGeometry(span, traceDuration);
  }
  const timelineRoot = sourceSpans[timelineRootIndex];
  const timelineDuration = Number(timelineRoot[DURATION]);
  if (!Number.isFinite(timelineDuration) || timelineDuration <= 0) {
    return globalTimelineBarGeometry(span, traceDuration);
  }

  return boundedTimelineBarGeometry(
    span,
    Number(timelineRoot[START]),
    timelineDuration,
    timelineRootIndex,
  );
}

function buildHierarchy(sourceSpans) {
  const sourceChildren = sourceSpans.map(() => []);
  const sourceRoots = [];
  sourceSpans.forEach((span, index) => {
    if (span[PARENT] >= 0 && span[PARENT] < sourceSpans.length) {
      sourceChildren[span[PARENT]].push(index);
    } else {
      sourceRoots.push(index);
    }
  });

  const reachable = new Set();
  function markReachable(start) {
    const pending = [start];
    while (pending.length) {
      const index = pending.pop();
      if (reachable.has(index)) continue;
      reachable.add(index);
      pending.push(...sourceChildren[index]);
    }
  }
  sourceRoots.forEach(markReachable);
  sourceSpans.forEach((span, index) => {
    if (!reachable.has(index)) {
      sourceRoots.push(index);
      markReachable(index);
    }
  });
  return { children: sourceChildren, roots: sourceRoots };
}

function defaultExpanded(
  sourceSpans,
  sourceChildren,
  scopes,
  collapsedScopes = COLLAPSED_SCOPES,
) {
  const result = new Set();
  sourceSpans.forEach((span, index) => {
    if (
      sourceChildren[index].length &&
      !collapsedScopes.has(scopes[span[SCOPE]])
    ) {
      result.add(index);
    }
  });
  return result;
}

function spanSearchText(span) {
  const attributes = Object.entries(span[ATTRS]).map(
    ([key, value]) =>
      `${key}=${typeof value === "string" ? value : JSON.stringify(value)}`,
  );
  return [span[NAME], span[STATUS_MESSAGE], ...attributes]
    .join(" ")
    .toLowerCase();
}

function matchingVisibility(sourceSpans, query, cache = []) {
  return filterVisibility(sourceSpans, { query }, cache);
}

function filterVisibility(
  sourceSpans,
  {
    query = "",
    failedOnly = false,
    topTestsOnly = false,
    minimumDurationNs = null,
    testSizes = new Set(),
    testPhase = null,
    scopes = [],
  } = {},
  cache = [],
) {
  const normalizedQuery = query.trim().toLowerCase();
  const normalizedSizes = new Set(
    [...testSizes]
      .map((size) => String(size).toLowerCase())
      .filter((size) => TEST_SIZES.includes(size)),
  );
  const hasMinimumDuration =
    Number.isFinite(minimumDurationNs) && minimumDurationNs >= 0;
  const selectedTestPhase =
    typeof testPhase === "string"
      ? parseTestPhaseSelection(testPhase)
      : testPhase &&
          testPhaseDefinition(testPhase.scope) &&
          (testPhase.name === null ||
            (typeof testPhase.name === "string" && testPhase.name))
        ? { scope: testPhase.scope, name: testPhase.name }
        : null;
  const hasSelectionFilter = Boolean(
    failedOnly ||
      topTestsOnly ||
      hasMinimumDuration ||
      normalizedSizes.size ||
      selectedTestPhase,
  );
  const active = Boolean(normalizedQuery || hasSelectionFilter);
  if (!active) return { visible: null, matches: 0 };

  const result = new Set();
  const directMatches = [];
  let matches = 0;
  sourceSpans.forEach((span, index) => {
    if (
      selectedTestPhase &&
      !spanMatchesTestPhase(span, scopes, selectedTestPhase)
    ) {
      return;
    }
    if (normalizedQuery) {
      const matchesQuery = (_candidate, candidateIndex) => {
        if (cache[candidateIndex] === undefined) {
          cache[candidateIndex] = spanSearchText(sourceSpans[candidateIndex]);
        }
        return cache[candidateIndex].includes(normalizedQuery);
      };
      const queryMatches = selectedTestPhase
        ? spanOrAncestorMatches(index, sourceSpans, matchesQuery)
        : matchesQuery(span, index);
      if (!queryMatches) return;
    }
    if (
      failedOnly &&
      (selectedTestPhase
        ? !testPhaseOrOwnerMatches(
            index,
            sourceSpans,
            scopes,
            selectedTestPhase,
            (candidate) => candidate[STATUS] === 2,
          )
        : span[STATUS] !== 2)
    ) {
      return;
    }
    if (topTestsOnly && longestTestRank(span) === null) return;
    if (hasMinimumDuration && span[DURATION] < minimumDurationNs) return;
    if (normalizedSizes.size) {
      const matchesSize = (candidate) => {
        const size = String(
          candidate[ATTRS]["test.size"] || "",
        ).toLowerCase();
        return normalizedSizes.has(size);
      };
      if (
        selectedTestPhase
          ? !testPhaseOrOwnerMatches(
              index,
              sourceSpans,
              scopes,
              selectedTestPhase,
              matchesSize,
            )
          : !matchesSize(span)
      ) {
        return;
      }
    }
    matches += 1;
    directMatches.push(index);
    let current = index;
    while (
      current >= 0 &&
      current < sourceSpans.length &&
      !result.has(current)
    ) {
      result.add(current);
      current = sourceSpans[current][PARENT];
    }
  });
  if (normalizedQuery && !hasSelectionFilter) {
    const sourceChildren = sourceSpans.map(() => []);
    sourceSpans.forEach((span, index) => {
      const parent = span[PARENT];
      if (parent >= 0 && parent < sourceSpans.length) {
        sourceChildren[parent].push(index);
      }
    });
    const seen = new Set(directMatches);
    const pending = [...directMatches];
    while (pending.length) {
      const index = pending.pop();
      sourceChildren[index].forEach((child) => {
        if (seen.has(child)) return;
        seen.add(child);
        result.add(child);
        pending.push(child);
      });
    }
  }
  return { visible: result, matches };
}

function flattenTraceRows({
  sourceSpans,
  sourceChildren,
  sourceRoots,
  sourceExpanded,
  sourceLimits = new Map(),
  sourceVisible = null,
  pageSize = PAGE_SIZE,
  maximumRows = INITIAL_ROW_BUDGET,
}) {
  const result = [];
  const seen = new Set();
  let spanRows = 0;
  let truncated = false;

  function addSpan(index, depth) {
    if (
      truncated ||
      seen.has(index) ||
      (sourceVisible && !sourceVisible.has(index))
    ) {
      return;
    }
    if (spanRows >= maximumRows) {
      truncated = true;
      return;
    }
    seen.add(index);
    result.push({ kind: "span", index, depth });
    spanRows += 1;
    const candidates = sourceVisible
      ? sourceChildren[index].filter((child) => sourceVisible.has(child))
      : sourceChildren[index];
    if (
      !candidates.length ||
      (!sourceVisible && !sourceExpanded.has(index))
    ) {
      return;
    }
    const limit = sourceLimits.has(index)
      ? sourceLimits.get(index)
      : pageSize;
    candidates.slice(0, limit).forEach((child) => addSpan(child, depth + 1));
    if (!truncated && candidates.length > limit) {
      result.push({
        kind: "more",
        index,
        depth: depth + 1,
        total: candidates.length,
        shown: limit,
      });
    }
  }

  sourceRoots.forEach((index) => addSpan(index, 0));
  return { items: result, spanRows, truncated };
}

function nextSelectedSpan(currentIndex, clickedIndex) {
  return currentIndex === clickedIndex ? null : clickedIndex;
}

function inlineDetailRows(sourceRows, selectedIndex) {
  const result = [];
  let inserted = false;
  sourceRows.forEach((row) => {
    result.push(row);
    if (!inserted && row.kind === "span" && row.index === selectedIndex) {
      result.push({
        kind: "detail",
        index: row.index,
        depth: row.depth,
      });
      inserted = true;
    }
  });
  return result;
}

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
  const hasTestSizes =
    testSizes instanceof Set
      ? testSizes.size > 0
      : Array.isArray(testSizes) && testSizes.length > 0;
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
    minimumDurationBadInput: Boolean(
      minimumDurationElement.validity?.badInput,
    ),
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
    testPhase: parseTestPhaseSelection(testPhaseElement.value),
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
  if (
    [...testPhaseElement.options].some(({ value }) => value === previousValue)
  ) {
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
  topTestsOnlyElement.setAttribute(
    "aria-pressed",
    String(cleared.topTestsOnly),
  );
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
  const result = filterVisibility(spans, currentFilters(), searchCache);
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
  if (expanded.has(index)) expanded.delete(index);
  else {
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

function spanRow(item) {
  const span = spans[item.index];
  const criticalPathTest = isCriticalPathTest(span);
  const durationRank = longestTestRank(span);
  const row = document.createElement("div");
  row.className = `span-row${span[STATUS] === 2 ? " error" : ""}${
    criticalPathTest ? " critical" : ""
  }${
    selected === item.index ? " selected" : ""
  }`;
  row.dataset.index = String(item.index);

  const nameCell = document.createElement("div");
  nameCell.className = "name-cell";
  nameCell.style.paddingLeft = `${Math.min(item.depth, 20) * 1.1}rem`;
  const toggle = document.createElement("button");
  toggle.className = "toggle";
  toggle.type = "button";
  const hasChildren = children[item.index].length > 0;
  const isOpen = visible ? hasChildren : expanded.has(item.index);
  toggle.textContent = hasChildren ? (isOpen ? "▾" : "▸") : "";
  toggle.disabled = !hasChildren || Boolean(visible);
  toggle.setAttribute(
    "aria-label",
    isOpen ? "Collapse span group" : "Expand span group",
  );
  toggle.setAttribute("aria-expanded", String(isOpen));
  toggle.addEventListener("click", () => toggleSpanGroup(item.index));
  const name = document.createElement(hasChildren ? "button" : "span");
  name.className = hasChildren ? "span-name group-name" : "span-name";
  if (hasChildren) {
    name.type = "button";
    name.disabled = Boolean(visible);
    name.setAttribute("aria-expanded", String(isOpen));
    name.addEventListener("click", () => toggleSpanGroup(item.index));
  }
  name.textContent = span[NAME];
  name.title = span[NAME];
  const childCount = document.createElement("span");
  if (hasChildren) {
    childCount.className = "child-count";
    childCount.textContent = directChildCountLabel(
      item.index,
      spans,
      children,
      model.c,
    );
    childCount.title = `${children[item.index].length.toLocaleString()} direct child spans; badge summarizes contained work`;
  }
  const critical = document.createElement("span");
  if (criticalPathTest) {
    critical.className = "critical-badge";
    critical.textContent = "★ critical";
    critical.title = "Test is on the ya critical path";
  }
  const longest = document.createElement("span");
  if (durationRank !== null) {
    longest.className = "longest-badge";
    longest.textContent = `#${durationRank} longest`;
    longest.title = `Ranked #${durationRank} among the ten longest tests in this ya make tests invocation`;
  }
  const metadata = document.createElement("button");
  metadata.className = "metadata-button";
  metadata.type = "button";
  metadata.textContent = "ⓘ";
  const metadataOpen = selected === item.index;
  metadata.title = `${metadataOpen ? "Hide" : "Show"} metadata for ${
    span[NAME]
  }`;
  metadata.setAttribute("aria-label", metadata.title);
  metadata.setAttribute("aria-expanded", String(metadataOpen));
  metadata.setAttribute("aria-controls", detailElementId(item.index));
  metadata.addEventListener("click", () => toggleSpanDetails(item.index));
  nameCell.append(toggle, name);
  if (hasChildren) nameCell.append(childCount);
  if (criticalPathTest) nameCell.append(critical);
  if (durationRank !== null) nameCell.append(longest);
  nameCell.append(metadata);

  const duration = document.createElement("span");
  duration.className = "duration";
  duration.textContent = formatDuration(span[DURATION]);
  const track = document.createElement("span");
  track.className = "track";
  const bar = document.createElement("span");
  bar.className = `bar ${timelineBarClass(
    model.c[span[SCOPE]],
    span[ATTRS],
  )}`;
  const timelineMode = timelineModeFromValue(timelineModeElement?.value);
  const geometry = timelineBarGeometry(
    item.index,
    spans,
    model.c,
    model.d,
    timelineMode,
  );
  bar.style.left = `${geometry.left}%`;
  bar.style.width = `${geometry.width}%`;
  if (geometry.width === 0) bar.hidden = true;
  if (geometry.relativeTo >= 0) {
    const localTimeline = spans[geometry.relativeTo];
    const localTimelineDescription =
      geometry.relativeTo === item.index
        ? `Local timeline root; full width is ${formatDuration(
            localTimeline[DURATION],
          )}. Descendants are positioned relative to this interval.`
        : `Local timeline relative to ${
            localTimeline[NAME]
          }; full width is ${formatDuration(localTimeline[DURATION])}.`;
    track.classList.add("local-timeline");
    track.dataset.relativeTo = String(geometry.relativeTo);
    track.title = localTimelineDescription;
    track.setAttribute("aria-label", localTimelineDescription);
  }
  track.append(bar);
  row.append(nameCell, duration, track);
  return row;
}

function moreRow(item) {
  const row = document.createElement("div");
  row.className = "more-row";
  row.style.setProperty(
    "--indent",
    `${Math.min(item.depth, 20) * 1.1 + 2.25}rem`,
  );
  const button = document.createElement("button");
  const load = groupLoadPlan(
    item.shown,
    item.total,
    selectedLoadSize(),
  );
  button.type = "button";
  button.textContent = `Load ${load.count.toLocaleString()} more in this group (${load.remaining.toLocaleString()} remaining)`;
  button.addEventListener("click", () => {
    limits.set(item.index, load.nextLimit);
    rowBudget = nextRowLimit(rowBudget, load.count);
    renderRows();
  });
  row.append(button);
  return row;
}

function renderRows() {
  const flattened = flattenRows();
  const fragment = document.createDocumentFragment();
  if (!flattened.items.length) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.id = "loading";
    empty.textContent = visible ? "No matching spans." : "No spans found.";
    fragment.append(empty);
  } else {
    inlineDetailRows(flattened.items, selected).forEach((item) => {
      if (item.kind === "span") fragment.append(spanRow(item));
      else if (item.kind === "detail") {
        fragment.append(spanDetailPanel(item.index));
      } else fragment.append(moreRow(item));
    });
  }
  rowsElement.replaceChildren(fragment);
  rowLoader.hidden = !flattened.truncated;
  rowLoadButton.disabled = !flattened.truncated;
  rowLoadButton.textContent = `Load next ${selectedLoadSize().toLocaleString()} rows`;
  rowStatus.textContent = `${flattened.spanRows.toLocaleString()} rows rendered${
    flattened.truncated ? "; more available" : ""
  }.`;
}

function valueText(value) {
  return typeof value === "string" ? value : JSON.stringify(value);
}

function linkableHttpUrl(value) {
  if (typeof value !== "string") return null;
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:"
      ? value
      : null;
  } catch (error) {
    return null;
  }
}

function safeHttpUrlPrefix(value) {
  const link = linkableHttpUrl(value);
  if (link === null) return null;
  try {
    const url = new URL(link);
    if (url.username || url.password || url.search || url.hash) return null;
    if (!url.pathname.endsWith("/")) url.pathname += "/";
    return url.toString();
  } catch (error) {
    return null;
  }
}

function safeArtifactPath(value) {
  if (
    typeof value !== "string" ||
    !value ||
    value.startsWith("/") ||
    value.includes("\\") ||
    value.includes("\u0000") ||
    value.includes("?") ||
    value.includes("#")
  ) {
    return null;
  }
  const segments = value.split("/");
  if (
    segments.some(
      (segment) => !segment || segment === "." || segment === "..",
    )
  ) {
    return null;
  }
  return segments
    .map((segment) =>
      encodeURIComponent(segment).replace(
        /[!'()*]/g,
        (character) =>
          `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
      ),
    )
    .join("/");
}

function artifactLinkForAttribute(span, key, value, links = {}) {
  if (!span || span[STATUS] !== 2) return null;
  let prefix = null;
  let suffix = "";
  if (/^ya\.(test|chunk)\.log\.[a-z0-9_]+\.path$/.test(key)) {
    prefix = links.testLog;
  } else if (/^ya\.(test|chunk)\.logs_directory\.path$/.test(key)) {
    prefix = links.testData;
    suffix = "/index.html";
  } else {
    return null;
  }
  const base = safeHttpUrlPrefix(prefix);
  const path = safeArtifactPath(value);
  if (base === null || path === null) return null;
  return new URL(`${path}${suffix}`, base).toString();
}

function attributeTable(values, sourceSpan = null) {
  if (!Object.keys(values).length) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = "No attributes";
    return empty;
  }
  const table = document.createElement("table");
  table.className = "attributes";
  Object.entries(values)
    .sort(([left], [right]) => left.localeCompare(right))
    .forEach(([key, value]) => {
      const row = document.createElement("tr");
      const heading = document.createElement("th");
      heading.textContent = key;
      const cell = document.createElement("td");
      const rendered = valueText(value);
      const absoluteLink = linkableHttpUrl(value);
      const linkTarget =
        absoluteLink ||
        artifactLinkForAttribute(sourceSpan, key, value, model?.u || {});
      if (linkTarget !== null) {
        const link = document.createElement("a");
        link.href = linkTarget;
        link.rel = "noopener noreferrer";
        link.textContent = absoluteLink === null ? `${rendered} ↗` : linkTarget;
        if (absoluteLink === null) link.title = linkTarget;
        cell.append(link);
      } else {
        cell.textContent = rendered;
      }
      row.append(heading, cell);
      table.append(row);
    });
  return table;
}

function heading(text, level = 3) {
  const element = document.createElement(`h${level}`);
  element.textContent = text;
  return element;
}

function detailElementId(index) {
  return `span-detail-${index}`;
}

function spanDetailPanel(index) {
  const span = spans[index];
  const panel = document.createElement("section");
  panel.className = "inline-detail";
  panel.id = detailElementId(index);
  panel.dataset.detailFor = String(index);
  panel.setAttribute("aria-label", `Metadata for ${span[NAME]}`);

  const head = document.createElement("div");
  head.className = "detail-head";
  const title = document.createElement("h2");
  title.textContent = span[NAME];
  const close = document.createElement("button");
  close.type = "button";
  close.textContent = "Close";
  close.setAttribute("aria-label", `Close metadata for ${span[NAME]}`);
  close.addEventListener("click", () => toggleSpanDetails(index));
  head.append(title, close);

  const facts = document.createElement("p");
  facts.className = "detail-facts";
  const parent =
    span[PARENT] >= 0
      ? spans[span[PARENT]][ID]
      : span[ORPHAN_PARENT] || "none";
  facts.append(
    `Duration: ${formatDuration(span[DURATION])} · Scope: ${
      model.c[span[SCOPE]]
    } · Status: ${span[STATUS]} ${span[STATUS_MESSAGE]}`,
    document.createElement("br"),
    `Trace ID: ${model.t[span[TRACE]]} · Span ID: ${
      span[ID]
    } · Parent: ${parent}`,
  );
  const content = document.createDocumentFragment();
  content.append(
    facts,
    heading("Attributes"),
    attributeTable(span[ATTRS], span),
  );
  if (span[EVENTS].length) {
    content.append(heading("Events"));
    const events = document.createElement("ul");
    events.className = "events";
    span[EVENTS].forEach((event) => {
      const item = document.createElement("li");
      item.append(
        `${formatDuration(Math.max(0, event[1]))} · ${event[0]}`,
      );
      item.append(attributeTable(event[2]));
      events.append(item);
    });
    content.append(events);
  }
  const resources = document.createElement("details");
  const resourceSummary = document.createElement("summary");
  resourceSummary.textContent = "Resource attributes";
  resources.append(
    resourceSummary,
    attributeTable(model.r[span[RESOURCE]]),
  );
  content.append(resources);
  panel.append(head, content);
  return panel;
}

function updateMetadataRow(index, open) {
  const row = rowsElement.querySelector(`.span-row[data-index="${index}"]`);
  if (!row) return null;
  row.classList.toggle("selected", open);
  const button = row.querySelector(".metadata-button");
  if (button) {
    const action = open ? "Hide" : "Show";
    const spanName = spans[index][NAME];
    button.title = `${action} metadata for ${spanName}`;
    button.setAttribute("aria-label", button.title);
    button.setAttribute("aria-expanded", String(open));
  }
  return row;
}

function toggleSpanDetails(index) {
  const previous = selected;
  const next = nextSelectedSpan(previous, index);
  if (previous !== null) {
    document.getElementById(detailElementId(previous))?.remove();
    updateMetadataRow(previous, false);
  }

  selected = next;
  if (next === null) return;
  const row = updateMetadataRow(next, true);
  if (!row) return;
  row.after(spanDetailPanel(next));
}

function browserLocalStorage() {
  try {
    return window.localStorage;
  } catch (_error) {
    return null;
  }
}

function currentNameColumnWidth() {
  return traceHeadElement.firstElementChild.getBoundingClientRect().width;
}

function setNameColumnWidth(width, persist = false) {
  const containerWidth = traceHeadElement.getBoundingClientRect().width;
  const clamped = clampNameColumnWidth(width, containerWidth);
  const maximum = clampNameColumnWidth(Number.MAX_SAFE_INTEGER, containerWidth);
  traceElement.style.setProperty("--name-column-width", `${clamped}px`);
  columnResizerElement.setAttribute("aria-valuemin", String(MIN_NAME_COLUMN_WIDTH_PX));
  columnResizerElement.setAttribute("aria-valuemax", String(maximum));
  columnResizerElement.setAttribute("aria-valuenow", String(clamped));
  columnResizerElement.setAttribute(
    "aria-valuetext",
    `${clamped} pixel name column`,
  );
  if (persist) writeStoredNameColumnWidth(browserLocalStorage(), clamped);
  return clamped;
}

function resizeNameColumnWithPointer(event) {
  if (!columnResizeState || event.pointerId !== columnResizeState.pointerId) {
    return;
  }
  const requested =
    columnResizeState.startWidth + event.clientX - columnResizeState.startX;
  columnResizeState.currentWidth = setNameColumnWidth(requested);
  event.preventDefault();
}

function finishNameColumnResize(event) {
  if (!columnResizeState || event.pointerId !== columnResizeState.pointerId) {
    return;
  }
  writeStoredNameColumnWidth(
    browserLocalStorage(),
    columnResizeState.currentWidth,
  );
  columnResizeState = null;
  document.body.classList.remove("resizing-columns");
}

function startNameColumnResize(event) {
  if (event.button !== 0) return;
  const width = currentNameColumnWidth();
  columnResizeState = {
    pointerId: event.pointerId,
    startX: event.clientX,
    startWidth: width,
    currentWidth: width,
  };
  document.body.classList.add("resizing-columns");
  event.preventDefault();
}

function resizeNameColumnWithKeyboard(event) {
  const direction = { ArrowLeft: -1, ArrowRight: 1 }[event.key];
  let requested;
  if (direction) {
    const step = event.shiftKey
      ? NAME_COLUMN_KEYBOARD_STEP_PX * 4
      : NAME_COLUMN_KEYBOARD_STEP_PX;
    requested = currentNameColumnWidth() + direction * step;
  } else if (event.key === "Home") {
    requested = MIN_NAME_COLUMN_WIDTH_PX;
  } else if (event.key === "End") {
    requested = Number.MAX_SAFE_INTEGER;
  } else {
    return;
  }
  setNameColumnWidth(requested, true);
  event.preventDefault();
}

function initializeColumnResizer() {
  const storedWidth = readStoredNameColumnWidth(browserLocalStorage());
  const mobile = window.matchMedia("(max-width:850px)").matches;
  if (storedWidth !== null) {
    setNameColumnWidth(storedWidth);
  } else if (!mobile) {
    setNameColumnWidth(currentNameColumnWidth());
  }

  columnResizerElement.addEventListener("pointerdown", startNameColumnResize);
  columnResizerElement.addEventListener("keydown", resizeNameColumnWithKeyboard);
  window.addEventListener("pointermove", resizeNameColumnWithPointer);
  window.addEventListener("pointerup", finishNameColumnResize);
  window.addEventListener("pointercancel", finishNameColumnResize);
  window.addEventListener("resize", () => {
    if (window.matchMedia("(max-width:850px)").matches) return;
    setNameColumnWidth(currentNameColumnWidth());
  });
}

function initialize(decoded) {
  model = decoded;
  spans = model.s;
  ({ children, roots } = buildHierarchy(spans));
  resetDefaults();
  populateTestPhaseOptions();
  filterElement.disabled = false;
  failedOnlyElement.disabled = false;
  topTestsOnlyElement.disabled = false;
  minimumDurationElement.disabled = false;
  testPhaseElement.disabled = false;
  testSizeElements.forEach((element) => {
    element.disabled = false;
  });
  rowLoadSizeElement.disabled = false;
  timelineModeElement.disabled = false;
  document.getElementById("expand").disabled = false;
  document.getElementById("collapse").disabled = false;
  updateFilterClearControls();
  renderRows();
}

function startTraceReport() {
  traceElement = document.getElementById("trace");
  traceHeadElement = document.getElementById("trace-head");
  columnResizerElement = document.getElementById("column-resizer");
  rowsElement = document.getElementById("rows");
  filterElement = document.getElementById("filter");
  clearFilterElement = document.getElementById("clear-filter");
  filterStatus = document.getElementById("filter-status");
  failedOnlyElement = document.getElementById("failed-only");
  topTestsOnlyElement = document.getElementById("top-tests-only");
  minimumDurationElement = document.getElementById("minimum-duration");
  clearMinimumDurationElement = document.getElementById(
    "clear-minimum-duration",
  );
  testPhaseElement = document.getElementById("test-phase");
  testSizeElements = document.querySelectorAll("[data-test-size]");
  clearFiltersElement = document.getElementById("clear-filters");
  rowLoadSizeElement = document.getElementById("row-load-size");
  timelineModeElement = document.getElementById("timeline-mode");
  rowLoader = document.getElementById("row-loader");
  rowLoadButton = document.getElementById("load-rows");
  rowStatus = document.getElementById("row-status");
  initializeColumnResizer();

  filterElement.addEventListener("input", () => {
    updateFilterClearControls();
    clearTimeout(filterTimer);
    filterTimer = setTimeout(applyFilter, 120);
  });
  minimumDurationElement.addEventListener("input", () => {
    updateFilterClearControls();
    clearTimeout(filterTimer);
    filterTimer = setTimeout(applyFilter, 120);
  });
  clearFilterElement.addEventListener("click", clearTextFilter);
  clearMinimumDurationElement.addEventListener(
    "click",
    clearMinimumDurationFilter,
  );
  clearFiltersElement.addEventListener("click", clearAllFilters);
  testPhaseElement.addEventListener("change", applyFilterImmediately);
  failedOnlyElement.addEventListener("click", () => {
    toggleFilter(failedOnlyElement);
  });
  topTestsOnlyElement.addEventListener("click", () => {
    toggleFilter(topTestsOnlyElement);
  });
  testSizeElements.forEach((element) => {
    element.addEventListener("click", () => toggleFilter(element));
  });
  rowLoadSizeElement.addEventListener("change", () => {
    renderRows();
  });
  timelineModeElement.addEventListener("change", () => {
    renderRows();
  });
  rowLoadButton.addEventListener("click", () => {
    rowBudget = nextRowLimit(rowBudget, selectedLoadSize());
    renderRows();
  });
  document.getElementById("expand").addEventListener("click", () => {
    spans.forEach((span, index) => {
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
  formatDuration,
  childCountLabel,
  directChildCountLabel,
  isCriticalPathTest,
  longestTestRank,
  parseMinimumDurationNs,
  filterControlActivity,
  clearedFilterControlState,
  encodeTestPhaseSelection,
  parseTestPhaseSelection,
  testPhaseOptions,
  timelineBarClass,
  timelineModeFromValue,
  timelineBarGeometry,
  buildHierarchy,
  defaultExpanded,
  spanSearchText,
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
