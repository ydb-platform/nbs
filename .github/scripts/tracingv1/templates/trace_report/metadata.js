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
  } catch (_error) {
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
  } catch (_error) {
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
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) {
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
      const label = document.createElement("th");
      label.textContent = key;
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
      row.append(label, cell);
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
  close.dataset.rowAction = "metadata";
  close.textContent = "Close";
  close.setAttribute("aria-label", `Close metadata for ${span[NAME]}`);
  head.append(title, close);

  const facts = document.createElement("p");
  facts.className = "detail-facts";
  const parent =
    span[PARENT] >= 0 ? spans[span[PARENT]][ID] : span[ORPHAN_PARENT] || "none";
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
  content.append(facts, heading("Attributes"), attributeTable(span[ATTRS], span));
  if (span[EVENTS].length) {
    content.append(heading("Events"));
    const events = document.createElement("ul");
    events.className = "events";
    span[EVENTS].forEach((event) => {
      const item = document.createElement("li");
      item.append(`${formatDuration(Math.max(0, event[1]))} · ${event[0]}`);
      item.append(attributeTable(event[2]));
      events.append(item);
    });
    content.append(events);
  }
  const resources = document.createElement("details");
  const resourceSummary = document.createElement("summary");
  resourceSummary.textContent = "Resource attributes";
  resources.append(resourceSummary, attributeTable(model.r[span[RESOURCE]]));
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
    button.title = `${action} metadata for ${spans[index][NAME]}`;
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
  if (row) row.after(spanDetailPanel(next));
}
