"use strict";

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
const INITIAL_RENDER_LIMIT = 2000;
const COLLAPSED_SCOPES = new Set(["ya.build", "ya.chunk"]);

let rowsElement;
let filterElement;
let filterStatus;
let detailPanel;
let detailTitle;
let detailContent;
let model;
let spans;
let children;
let roots;
let expanded;
let limits;
let visible;
let selected = null;
let searchCache = [];
let renderLimit = INITIAL_RENDER_LIMIT;

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
  const normalized = query.trim().toLowerCase();
  if (!normalized) return { visible: null, matches: 0 };

  const result = new Set();
  let matches = 0;
  sourceSpans.forEach((span, index) => {
    if (!cache[index]) cache[index] = spanSearchText(span);
    if (!cache[index].includes(normalized)) return;
    matches += 1;
    let current = index;
    while (current >= 0 && !result.has(current)) {
      result.add(current);
      current = sourceSpans[current][PARENT];
    }
  });
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
  maximumRows = INITIAL_RENDER_LIMIT,
}) {
  const result = [];
  const seen = new Set();
  let truncated = false;

  function addSpan(index, depth) {
    if (
      truncated ||
      seen.has(index) ||
      (sourceVisible && !sourceVisible.has(index))
    ) {
      return;
    }
    if (result.length >= maximumRows) {
      truncated = true;
      return;
    }
    seen.add(index);
    result.push({ kind: "span", index, depth });
    const candidates = sourceVisible
      ? sourceChildren[index].filter((child) => sourceVisible.has(child))
      : sourceChildren[index];
    if (
      !candidates.length ||
      (!sourceVisible && !sourceExpanded.has(index))
    ) {
      return;
    }
    const limit = sourceLimits.get(index) || pageSize;
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
  return { items: result, truncated };
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
  renderLimit = INITIAL_RENDER_LIMIT;
}

function applyFilter() {
  const query = filterElement.value.trim();
  if (!query) {
    visible = null;
    filterStatus.textContent = "";
    resetDefaults();
    renderRows();
    return;
  }
  renderLimit = INITIAL_RENDER_LIMIT;
  const result = matchingVisibility(spans, query, searchCache);
  visible = result.visible;
  filterStatus.textContent = `${result.matches.toLocaleString()} matching spans`;
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
    maximumRows: renderLimit,
  });
}

function toggleSpanGroup(index) {
  if (visible || !children[index].length) return;
  if (expanded.has(index)) expanded.delete(index);
  else expanded.add(index);
  renderRows();
}

function spanRow(item) {
  const span = spans[item.index];
  const row = document.createElement("div");
  row.className = `span-row${span[STATUS] === 2 ? " error" : ""}${
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
  const metadata = document.createElement("button");
  metadata.className = "metadata-button";
  metadata.type = "button";
  metadata.textContent = "ⓘ";
  metadata.title = `Show metadata for ${span[NAME]}`;
  metadata.setAttribute("aria-label", metadata.title);
  metadata.addEventListener("click", () => showSpan(item.index));
  nameCell.append(toggle, name, metadata);

  const duration = document.createElement("span");
  duration.className = "duration";
  duration.textContent = formatDuration(span[DURATION]);
  const track = document.createElement("span");
  track.className = "track";
  const bar = document.createElement("span");
  bar.className = "bar";
  bar.style.left = `${(100 * Math.max(0, span[START])) / model.d}%`;
  bar.style.width = `${Math.max(
    0.15,
    (100 * span[DURATION]) / model.d,
  )}%`;
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
  const remaining = item.total - item.shown;
  button.type = "button";
  button.textContent = `Load ${Math.min(
    PAGE_SIZE,
    remaining,
  ).toLocaleString()} more (${remaining.toLocaleString()} remaining)`;
  button.addEventListener("click", () => {
    limits.set(item.index, item.shown + PAGE_SIZE);
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
    flattened.items.forEach((item) =>
      fragment.append(item.kind === "span" ? spanRow(item) : moreRow(item)),
    );
    if (flattened.truncated) {
      const limitRow = document.createElement("div");
      limitRow.className = "more-row";
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = `Render up to ${(
        renderLimit + INITIAL_RENDER_LIMIT
      ).toLocaleString()} visible rows`;
      button.addEventListener("click", () => {
        renderLimit += INITIAL_RENDER_LIMIT;
        renderRows();
      });
      limitRow.append(button);
      fragment.append(limitRow);
    }
  }
  rowsElement.replaceChildren(fragment);
}

function valueText(value) {
  return typeof value === "string" ? value : JSON.stringify(value);
}

function attributeTable(values) {
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
      let linked = false;
      if (typeof value === "string") {
        try {
          const url = new URL(value);
          if (url.protocol === "http:" || url.protocol === "https:") {
            const link = document.createElement("a");
            link.href = value;
            link.rel = "noopener noreferrer";
            link.textContent = value;
            cell.append(link);
            linked = true;
          }
        } catch (error) {
          // Non-URL attributes are rendered as plain text.
        }
      }
      if (!linked) cell.textContent = rendered;
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

function showSpan(index) {
  selected = index;
  const span = spans[index];
  detailTitle.textContent = span[NAME];
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
  content.append(facts, heading("Attributes"), attributeTable(span[ATTRS]));
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
  detailContent.replaceChildren(content);
  detailPanel.hidden = false;
  renderRows();
}

function initialize(decoded) {
  model = decoded;
  spans = model.s;
  ({ children, roots } = buildHierarchy(spans));
  resetDefaults();
  filterElement.disabled = false;
  document.getElementById("expand").disabled = false;
  document.getElementById("collapse").disabled = false;
  renderRows();
}

function startTraceReport() {
  rowsElement = document.getElementById("rows");
  filterElement = document.getElementById("filter");
  filterStatus = document.getElementById("filter-status");
  detailPanel = document.getElementById("detail-panel");
  detailTitle = document.getElementById("detail-title");
  detailContent = document.getElementById("detail-content");

  let filterTimer;
  filterElement.addEventListener("input", () => {
    clearTimeout(filterTimer);
    filterTimer = setTimeout(applyFilter, 120);
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
  document.getElementById("detail-close").addEventListener("click", () => {
    selected = null;
    detailPanel.hidden = true;
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
  INITIAL_RENDER_LIMIT,
  COLLAPSED_SCOPES,
  formatDuration,
  buildHierarchy,
  defaultExpanded,
  spanSearchText,
  matchingVisibility,
  flattenTraceRows,
};

if (typeof module === "object" && module.exports) {
  module.exports = traceReportApi;
} else {
  startTraceReport();
}
