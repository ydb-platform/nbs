function rowOrderFromValue(value) {
  if (value === ROW_ORDERS.DURATION) return value;
  const selection = parseTestPhaseSelection(value);
  return selection
    ? encodeTestPhaseSelection(selection.scope, selection.name)
    : ROW_ORDERS.TREE;
}

function rowOrderIsFlat(value) {
  if (rowOrderFromValue(value) === ROW_ORDERS.TREE) return false;
  const selection = parseTestPhaseSelection(value);
  return !testPhaseDefinition(selection?.scope)?.expandable;
}

function durationOrderedIndexes(sourceIndex, order, rawFilters = {}) {
  const selection = parseTestPhaseSelection(order);
  const grouped = Boolean(testPhaseDefinition(selection?.scope)?.expandable);
  const filters = normalizeFilterSpec({
    ...rawFilters,
    scopes: sourceIndex.scopes,
  });
  const result = filterVisibility(
    sourceIndex.spans,
    { ...filters, testPhase: filters.testPhase || (grouped ? null : selection) },
    sourceIndex.searchTexts,
    sourceIndex,
  );
  // Ordered groups must keep ancestors of matching tests or phases. Applying
  // the order scope as a filter would discard those descendant matches.
  const matching =
    grouped || filters.testPhase || (!selection && !filters.hasSelection)
      ? result.visible
      : result.matching;
  return sourceIndex.spans
    .flatMap((span, index) =>
      (!selection || spanMatchesTestPhase(span, sourceIndex.scopes, selection)) &&
      (!matching || matching.has(index))
        ? [index]
        : [],
    )
    .sort(
      (left, right) =>
        displayedDurationNs(sourceIndex.spans[right], sourceIndex.scope(right)) -
          displayedDurationNs(sourceIndex.spans[left], sourceIndex.scope(left)) ||
        left - right,
    );
}

function flatOrderedRows(sourceRows, maximumRows = INITIAL_ROW_BUDGET) {
  const shown = sourceRows.slice(0, maximumRows);
  return {
    items: shown.map((index) => ({ kind: "span", index, depth: 0, flat: true })),
    spanRows: shown.length,
    truncated: sourceRows.length > shown.length,
  };
}
