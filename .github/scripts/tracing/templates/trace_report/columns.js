function browserLocalStorage() {
  try {
    return window.localStorage;
  } catch (_error) {
    return null;
  }
}

function initializeStickyTraceHeader() {
  const toolbar = document.querySelector(".toolbar");
  const updateOffset = () => {
    document.documentElement.style.setProperty(
      "--toolbar-height",
      `${toolbar.getBoundingClientRect().height}px`,
    );
  };
  updateOffset();
  if (typeof ResizeObserver === "function") {
    toolbarResizeObserver = new ResizeObserver(updateOffset);
    toolbarResizeObserver.observe(toolbar);
  } else {
    window.addEventListener("resize", updateOffset);
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
  columnResizerElement.setAttribute(
    "aria-valuemin",
    String(MIN_NAME_COLUMN_WIDTH_PX),
  );
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
  if (storedWidth !== null) setNameColumnWidth(storedWidth);
  else if (!mobile) setNameColumnWidth(currentNameColumnWidth());

  columnResizerElement.addEventListener("pointerdown", startNameColumnResize);
  columnResizerElement.addEventListener("keydown", resizeNameColumnWithKeyboard);
  window.addEventListener("pointermove", resizeNameColumnWithPointer);
  window.addEventListener("pointerup", finishNameColumnResize);
  window.addEventListener("pointercancel", finishNameColumnResize);
  window.addEventListener("resize", () => {
    if (!window.matchMedia("(max-width:850px)").matches) {
      setNameColumnWidth(currentNameColumnWidth());
    }
  });
}
