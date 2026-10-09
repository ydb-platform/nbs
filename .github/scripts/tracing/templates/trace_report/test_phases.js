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
