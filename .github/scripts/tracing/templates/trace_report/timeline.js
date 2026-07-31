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
