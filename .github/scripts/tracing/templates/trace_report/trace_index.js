class TraceIndex {
  constructor(sourceSpans, scopes = [], searchTexts = []) {
    this.spans = sourceSpans;
    this.scopes = scopes;
    this.searchTexts = searchTexts;
    ({ children: this.children, roots: this.roots } =
      TraceIndex.buildHierarchy(sourceSpans));
    this.phaseOwners = new Map(
      TEST_PHASE_DEFINITIONS.map(({ scope, ownerScopes }) => {
        const owners = new Set(ownerScopes);
        return [
          scope,
          sourceSpans.map((_span, index) =>
            this.scope(index) === scope
              ? this.nearestAncestorInScopes(index, owners)
              : -1,
          ),
        ];
      }),
    );
  }

  static buildHierarchy(sourceSpans) {
    const sourceChildren = sourceSpans.map(() => []);
    const sourceRoots = [];
    sourceSpans.forEach((span, index) => {
      const parent = span[PARENT];
      if (parent >= 0 && parent < sourceSpans.length) {
        sourceChildren[parent].push(index);
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
    sourceSpans.forEach((_span, index) => {
      if (!reachable.has(index)) {
        sourceRoots.push(index);
        markReachable(index);
      }
    });
    return { children: sourceChildren, roots: sourceRoots };
  }

  scope(index) {
    return this.scopes[this.spans[index][SCOPE]];
  }

  ancestors(index, includeSelf = true) {
    const result = [];
    const seen = new Set();
    let current = includeSelf ? index : this.spans[index]?.[PARENT];
    while (
      Number.isInteger(current) &&
      current >= 0 &&
      current < this.spans.length &&
      !seen.has(current)
    ) {
      result.push(current);
      seen.add(current);
      current = this.spans[current][PARENT];
    }
    return result;
  }

  nearestAncestorInScopes(index, ownerScopes) {
    const seen = new Set([index]);
    let current = this.spans[index]?.[PARENT];
    while (
      Number.isInteger(current) &&
      current >= 0 &&
      current < this.spans.length &&
      !seen.has(current)
    ) {
      if (ownerScopes.has(this.scope(current))) return current;
      seen.add(current);
      current = this.spans[current][PARENT];
    }
    return -1;
  }

  phaseOwner(index, phaseScope) {
    return this.phaseOwners.get(phaseScope)?.[index] ?? -1;
  }

  searchText(index) {
    if (this.searchTexts[index] === undefined) {
      this.searchTexts[index] = spanSearchText(this.spans[index]);
    }
    return this.searchTexts[index];
  }

  addAncestors(target, index) {
    this.ancestors(index).forEach((candidate) => target.add(candidate));
  }

  addDescendants(target, startIndexes) {
    const seen = new Set(startIndexes);
    const pending = [...startIndexes];
    while (pending.length) {
      const index = pending.pop();
      this.children[index].forEach((child) => {
        if (seen.has(child)) return;
        seen.add(child);
        target.add(child);
        pending.push(child);
      });
    }
  }
}

function buildHierarchy(sourceSpans) {
  return TraceIndex.buildHierarchy(sourceSpans);
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
