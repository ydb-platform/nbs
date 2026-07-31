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
