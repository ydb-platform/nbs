function spanRow(item) {
  const span = spans[item.index];
  const criticalPath = criticalPathBadge(span, model.c[span[SCOPE]]);
  const durationRank = longestTestRank(span);
  const row = document.createElement("div");
  row.className = `span-row${span[STATUS] === 2 ? " error" : ""}${
    criticalPath ? " critical" : ""
  }${selected === item.index ? " selected" : ""}`;
  row.dataset.index = String(item.index);

  const nameCell = document.createElement("div");
  nameCell.className = "name-cell";
  nameCell.style.paddingLeft = `${Math.min(item.depth, 20) * 1.1}rem`;
  const toggle = document.createElement("button");
  toggle.className = "toggle";
  toggle.type = "button";
  toggle.dataset.rowAction = "group";
  const hasChildren = children[item.index].length > 0;
  const isOpen = visible ? hasChildren : expanded.has(item.index);
  toggle.textContent = hasChildren ? (isOpen ? "▾" : "▸") : "";
  toggle.disabled = !hasChildren || Boolean(visible);
  toggle.setAttribute(
    "aria-label",
    isOpen ? "Collapse span group" : "Expand span group",
  );
  toggle.setAttribute("aria-expanded", String(isOpen));
  const name = document.createElement(hasChildren ? "button" : "span");
  name.className = hasChildren ? "span-name group-name" : "span-name";
  if (hasChildren) {
    name.type = "button";
    name.dataset.rowAction = "group";
    name.disabled = Boolean(visible);
    name.setAttribute("aria-expanded", String(isOpen));
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
  if (criticalPath) {
    critical.className = "critical-badge";
    critical.textContent = criticalPath.label;
    critical.title = criticalPath.title;
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
  metadata.dataset.rowAction = "metadata";
  metadata.textContent = "ⓘ";
  const metadataOpen = selected === item.index;
  metadata.title = `${metadataOpen ? "Hide" : "Show"} metadata for ${
    span[NAME]
  }`;
  metadata.setAttribute("aria-label", metadata.title);
  metadata.setAttribute("aria-expanded", String(metadataOpen));
  metadata.setAttribute("aria-controls", detailElementId(item.index));
  nameCell.append(toggle, name);
  if (hasChildren) nameCell.append(childCount);
  if (criticalPath) nameCell.append(critical);
  if (durationRank !== null) nameCell.append(longest);
  nameCell.append(metadata);

  const duration = document.createElement("span");
  duration.className = "duration";
  duration.textContent = formatDuration(span[DURATION]);
  const track = document.createElement("span");
  track.className = "track";
  const bar = document.createElement("span");
  bar.className = `bar ${timelineBarClass(model.c[span[SCOPE]], span[ATTRS])}`;
  const geometry = timelineBarGeometry(
    item.index,
    spans,
    model.c,
    model.d,
    timelineModeFromValue(timelineModeElement?.value),
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
  const load = groupLoadPlan(item.shown, item.total, selectedLoadSize());
  button.type = "button";
  button.dataset.rowAction = "load-group";
  button.dataset.index = String(item.index);
  button.dataset.nextLimit = String(load.nextLimit);
  button.dataset.loadCount = String(load.count);
  button.textContent = `Load ${load.count.toLocaleString()} more in this group (${load.remaining.toLocaleString()} remaining)`;
  row.append(button);
  return row;
}

function handleRowsClick(event) {
  const actionElement = event.target.closest?.("[data-row-action]");
  if (!actionElement || !rowsElement.contains(actionElement)) return;
  const container = actionElement.closest("[data-index], [data-detail-for]");
  const index = Number(
    actionElement.dataset.index ||
      container?.dataset.index ||
      container?.dataset.detailFor,
  );
  if (!Number.isInteger(index) || index < 0 || index >= spans.length) return;
  if (actionElement.dataset.rowAction === "group") {
    toggleSpanGroup(index);
  } else if (actionElement.dataset.rowAction === "metadata") {
    toggleSpanDetails(index);
  } else if (actionElement.dataset.rowAction === "load-group") {
    const nextLimit = Number(actionElement.dataset.nextLimit);
    const count = Number(actionElement.dataset.loadCount);
    if (!Number.isFinite(nextLimit) || !Number.isFinite(count)) return;
    limits.set(index, nextLimit);
    rowBudget = nextRowLimit(rowBudget, count);
    renderRows();
  }
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
      else if (item.kind === "detail") fragment.append(spanDetailPanel(item.index));
      else fragment.append(moreRow(item));
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
