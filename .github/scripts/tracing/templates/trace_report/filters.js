function normalizedTestPhase(testPhase) {
  if (typeof testPhase === "string") return parseTestPhaseSelection(testPhase);
  if (
    !testPhase ||
    !testPhaseDefinition(testPhase.scope) ||
    (testPhase.name !== null &&
      (typeof testPhase.name !== "string" ||
        !testPhase.name ||
        !testPhaseDefinition(testPhase.scope).attribute))
  ) {
    return null;
  }
  return { scope: testPhase.scope, name: testPhase.name };
}

function normalizeFilterSpec(
  {
    query = "",
    failedOnly = false,
    topTestsOnly = false,
    minimumDurationNs = null,
    testSizes = new Set(),
    testPhase = null,
    scopes = [],
  } = {},
) {
  const sizes = testSizes?.[Symbol.iterator] ? [...testSizes] : [];
  const normalized = {
    query: String(query ?? "").trim().toLowerCase(),
    failedOnly: Boolean(failedOnly),
    topTestsOnly: Boolean(topTestsOnly),
    minimumDurationNs:
      Number.isFinite(minimumDurationNs) && minimumDurationNs >= 0
        ? minimumDurationNs
        : null,
    testSizes: new Set(
      sizes
        .map((size) => String(size).toLowerCase())
        .filter((size) => TEST_SIZES.includes(size)),
    ),
    testPhase: normalizedTestPhase(testPhase),
    scopes,
  };
  normalized.hasSelection = Boolean(
    normalized.failedOnly ||
      normalized.topTestsOnly ||
      normalized.minimumDurationNs !== null ||
      normalized.testSizes.size ||
      normalized.testPhase,
  );
  normalized.active = Boolean(normalized.query || normalized.hasSelection);
  return normalized;
}

function matchingVisibility(sourceSpans, query, cache = []) {
  return filterVisibility(sourceSpans, { query }, cache);
}

function phaseOrOwnerMatches(index, traceIndex, selection, predicate) {
  if (predicate(traceIndex.spans[index], index)) return true;
  const owner = traceIndex.phaseOwner(index, selection.scope);
  return owner >= 0 && predicate(traceIndex.spans[owner], owner);
}

function filterVisibility(
  sourceSpans,
  rawFilters = {},
  cache = [],
  sourceIndex = null,
) {
  const filters = normalizeFilterSpec(rawFilters);
  if (!filters.active) return { visible: null, matches: 0 };

  const index = sourceIndex || new TraceIndex(sourceSpans, filters.scopes, cache);
  const result = new Set();
  const directMatches = [];
  const matchesQuery = (_span, candidate) =>
    index.searchText(candidate).includes(filters.query);
  const matchesSize = (span) =>
    filters.testSizes.has(String(span[ATTRS]["test.size"] || "").toLowerCase());
  const matchesTopTest = (span) => longestTestRank(span) !== null;
  const matchesQueryContext = (spanIndex) =>
    index.ancestors(spanIndex).some((candidate) =>
      matchesQuery(sourceSpans[candidate], candidate),
    );
  const grouped = Boolean(
    testPhaseDefinition(filters.testPhase?.scope)?.expandable,
  );
  let matchingGroupAncestors = null;
  if (
    grouped &&
    (filters.query ||
      filters.failedOnly ||
      filters.topTestsOnly ||
      filters.testSizes.size)
  ) {
    // Match all non-duration predicates on one span before selecting its
    // containing groups. Different siblings cannot supply separate predicates.
    matchingGroupAncestors = new Set();
    sourceSpans.forEach((span, spanIndex) => {
      if (filters.topTestsOnly && !matchesTopTest(span)) return;
      const selection = { scope: index.scope(spanIndex) };
      if (filters.failedOnly && !phaseOrOwnerMatches(
        spanIndex, index, selection, (candidate) => candidate[STATUS] === 2,
      )) return;
      if (filters.testSizes.size && !phaseOrOwnerMatches(
        spanIndex, index, selection, matchesSize,
      )) return;
      if (filters.query && !matchesQueryContext(spanIndex)) return;
      index.addAncestors(matchingGroupAncestors, spanIndex);
    });
  }
  const topTestAncestors = new Set();
  if (!grouped && filters.topTestsOnly && filters.testPhase) {
    sourceSpans.forEach((span, spanIndex) => {
      if (matchesTopTest(span)) {
        index
          .ancestors(spanIndex)
          .forEach((ancestor) => topTestAncestors.add(ancestor));
      }
    });
  }

  const predicates = [
    filters.testPhase &&
      ((span) =>
        spanMatchesTestPhase(span, filters.scopes, filters.testPhase)),
    matchingGroupAncestors &&
      ((_span, spanIndex) => matchingGroupAncestors.has(spanIndex)),
    !grouped &&
      filters.query &&
      ((span, spanIndex) =>
        filters.testPhase
          ? matchesQueryContext(spanIndex)
          : matchesQuery(span, spanIndex)),
    !grouped &&
      filters.failedOnly &&
      ((span, spanIndex) =>
        filters.testPhase
          ? phaseOrOwnerMatches(
              spanIndex,
              index,
              filters.testPhase,
              (candidate) => candidate[STATUS] === 2,
            )
          : span[STATUS] === 2),
    !grouped &&
      filters.topTestsOnly &&
      ((span, spanIndex) =>
        filters.testPhase
          ? matchesTopTest(span) ||
            topTestAncestors.has(spanIndex) ||
            topTestAncestors.has(
              index.phaseOwner(spanIndex, filters.testPhase.scope),
            )
          : matchesTopTest(span)),
    filters.minimumDurationNs !== null &&
      ((span) =>
        displayedDurationNs(span, filters.scopes[span[SCOPE]]) >=
        filters.minimumDurationNs),
    !grouped &&
      filters.testSizes.size &&
      ((span, spanIndex) =>
        filters.testPhase
          ? phaseOrOwnerMatches(
              spanIndex,
              index,
              filters.testPhase,
              matchesSize,
            )
          : matchesSize(span)),
  ].filter(Boolean);

  sourceSpans.forEach((span, spanIndex) => {
    if (!predicates.every((predicate) => predicate(span, spanIndex))) return;
    directMatches.push(spanIndex);
    index.addAncestors(result, spanIndex);
  });

  if (filters.query && !filters.hasSelection) {
    // A group's name identifies its subtree. Metadata (such as a build-wide
    // critical-path summary) can mention one target without identifying every
    // child as part of that target.
    const matchingGroups = directMatches.filter((spanIndex) =>
      sourceSpans[spanIndex][NAME].toLowerCase().includes(filters.query),
    );
    index.addDescendants(result, matchingGroups);
  }
  return {
    visible: result,
    matching: new Set(directMatches),
    matches: directMatches.length,
  };
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
    if (!candidates.length || (!sourceVisible && !sourceExpanded.has(index))) {
      return;
    }
    const limit = sourceLimits.has(index) ? sourceLimits.get(index) : pageSize;
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
      result.push({ kind: "detail", index: row.index, depth: row.depth });
      inserted = true;
    }
  });
  return result;
}
