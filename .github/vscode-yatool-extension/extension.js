const path = require("path");
const vscode = require("vscode");

const MACRO_KIND = {
  DATA: "data",
  INCLUDE: "file",
  PEERDIR: "module-directory-root-relative",
  RECURSE: "module-directory-current-relative",
  RECURSE_FOR_TESTS: "module-directory-current-relative",
  RECURSE_ROOT_RELATIVE: "module-directory-root-relative",
};
const PATH_MACROS = new Set(Object.keys(MACRO_KIND));
const UNSUPPORTED_DATA_PREFIXES = ["sbr://", "ext:", "http://", "https://"];
const RESOURCE_MACROS = new Set(["FROM_SANDBOX"]);
const RESOURCE_MAPPING_FILES = ["build/ext_mapping.conf.json", "build/mapping.conf.json"];
const MACRO_DOCS = {
  DATA: {
    usage: "DATA(arcadia/path | sbr://resource | ...)",
    text: "Adds files or directories to test data. Local `arcadia/...` paths are copied into the test environment.",
  },
  DEPENDS: {
    usage: "DEPENDS(path1 [path2...])",
    text: "Adds build dependencies that are needed by tests or runtime workflows but are not linked as normal library peers.",
  },
  FROM_SANDBOX: {
    usage: "FROM_SANDBOX([FILE] resource_id [RENAME <resource files>] OUT_[NOAUTO] <output files> ...)",
    text: "Downloads a Sandbox/resource-mapping resource, optionally unpacks it, and declares output files for the build graph.",
  },
  INCLUDE: {
    usage: "INCLUDE(filename)",
    text: "Reads another makelist fragment. Relative paths are resolved from the current file; `${ARCADIA_ROOT}` is resolved from the workspace root.",
  },
  PEERDIR: {
    usage: "PEERDIR(dirs...)",
    text: "Declares module dependencies. Library peers are linked into executable/shared targets that depend on this module.",
  },
  PY3TEST: {
    usage: "PY3TEST([name])",
    text: "Defines a Python 3 pytest-based test module. It is compatible with Python 3-tagged modules.",
  },
  RECURSE: {
    usage: "RECURSE(dirs...)",
    text: "Adds child directories to the build traversal. Arguments are relative to the current `ya.make` directory.",
  },
  RECURSE_FOR_TESTS: {
    usage: "RECURSE_FOR_TESTS(dirs...)",
    text: "Adds directories to traversal when tests are requested. Use test traversal flags to include these in dependency dumps/build traversal.",
  },
  RECURSE_ROOT_RELATIVE: {
    usage: "RECURSE_ROOT_RELATIVE(dirlist)",
    text: "Adds directories to build traversal using paths relative to `${ARCADIA_ROOT}`.",
  },
  SET: {
    usage: "SET(varname value)",
    text: "Sets a ymake variable in the current configuration scope.",
  },
  SPLIT_FACTOR: {
    usage: "SPLIT_FACTOR(x)",
    text: "Sets the number of chunks for parallel test execution. With test modules, it works with `FORK_TESTS()` / `FORK_SUBTESTS()` and may imply test forking.",
  },
  TEST_SRCS: {
    usage: "TEST_SRCS(files...)",
    text: "Declares source files containing tests for the current test module.",
  },
};

let diagnostics;
let resourceMappingCache;

function activate(context) {
  diagnostics = vscode.languages.createDiagnosticCollection("yatool");
  context.subscriptions.push(diagnostics);

  const selector = [
    { language: "yamake" },
    { pattern: "**/ya.make" },
    { pattern: "**/*.inc" },
    { pattern: "**/*.make.inc" },
  ];

  context.subscriptions.push(
    vscode.languages.registerDocumentLinkProvider(selector, {
      provideDocumentLinks(document) {
        return collectPathRefs(document)
          .filter((ref) => ref.target)
          .map((ref) => {
            const link = new vscode.DocumentLink(ref.range, ref.linkTarget || ref.target);
            link.tooltip = `Open ${ref.displayTarget}`;
            return link;
          });
      },
    }),
  );

  context.subscriptions.push(
    vscode.languages.registerDefinitionProvider(selector, {
      async provideDefinition(document, position) {
        const ref = findResourceRefAtPosition(document, position);
        if (!ref) {
          return undefined;
        }
        const mapping = await findResourceMapping(document, ref.value);
        return mapping ? mapping.location : undefined;
      },
    }),
  );

  context.subscriptions.push(
    vscode.languages.registerHoverProvider(selector, {
      async provideHover(document, position) {
        const macro = findMacroAtPosition(document, position);
        if (macro) {
          return new vscode.Hover(formatMacroHover(macro));
        }

        const resource = findResourceRefAtPosition(document, position);
        if (resource) {
          const mapping = await findResourceMapping(document, resource.value);
          if (mapping) {
            return new vscode.Hover(formatResourceHover(resource.value, mapping));
          }
        }

        return undefined;
      },
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("yatool.refreshDiagnostics", () => {
      for (const document of vscode.workspace.textDocuments) {
        if (isYaMakeDocument(document)) {
          validateDocument(document);
        }
      }
    }),
  );

  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((document) => {
      if (isYaMakeDocument(document)) {
        validateDocument(document);
      }
    }),
    vscode.workspace.onDidChangeTextDocument((event) => {
      if (isYaMakeDocument(event.document)) {
        validateDocument(event.document);
      }
    }),
    vscode.workspace.onDidCloseTextDocument((document) => diagnostics.delete(document.uri)),
    vscode.workspace.onDidSaveTextDocument((document) => {
      if (isResourceMappingDocument(document)) {
        resourceMappingCache = undefined;
      }
    }),
  );

  for (const document of vscode.workspace.textDocuments) {
    if (isYaMakeDocument(document)) {
      validateDocument(document);
    }
  }
}

function deactivate() {}

function isYaMakeDocument(document) {
  const base = path.posix.basename(document.uri.path);
  return document.languageId === "yamake" || base === "ya.make" || base.endsWith(".inc");
}

function isResourceMappingDocument(document) {
  return RESOURCE_MAPPING_FILES.some((relativePath) => document.uri.path.endsWith(`/${relativePath}`));
}

async function validateDocument(document) {
  const parsed = parseMakelist(document.getText());
  const refs = collectPathRefs(document, parsed);
  const foundDiagnostics = [];

  for (const error of parsed.errors) {
    foundDiagnostics.push(
      new vscode.Diagnostic(
        new vscode.Range(document.positionAt(error.start), document.positionAt(error.end)),
        error.message,
        vscode.DiagnosticSeverity.Error,
      ),
    );
  }

  for (const ref of refs) {
    if (ref.unresolvedReason) {
      foundDiagnostics.push(
        new vscode.Diagnostic(ref.range, ref.unresolvedReason, vscode.DiagnosticSeverity.Information),
      );
      continue;
    }

    if (!ref.target) {
      continue;
    }

    try {
      const stat = await vscode.workspace.fs.stat(ref.target);
      if (ref.kind === "file" && isDirectory(stat)) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, "INCLUDE path resolves to a directory, expected a file.", vscode.DiagnosticSeverity.Error),
        );
      } else if (ref.kind.startsWith("module-directory") && !isDirectory(stat)) {
        foundDiagnostics.push(
          new vscode.Diagnostic(ref.range, `${ref.macro} path resolves to a file, expected a directory.`, vscode.DiagnosticSeverity.Error),
        );
      } else if (ref.kind.startsWith("module-directory")) {
        try {
          await vscode.workspace.fs.stat(vscode.Uri.joinPath(ref.target, "ya.make"));
        } catch (error) {
          foundDiagnostics.push(
            new vscode.Diagnostic(ref.range, `${ref.macro} path does not contain ya.make: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Warning),
          );
        }
      }
    } catch (error) {
      foundDiagnostics.push(
        new vscode.Diagnostic(ref.range, `Path does not exist: ${ref.displayTarget}`, vscode.DiagnosticSeverity.Error),
      );
    }
  }

  diagnostics.set(document.uri, foundDiagnostics);
}

function isDirectory(stat) {
  return Boolean(stat.type & vscode.FileType.Directory);
}

function collectPathRefs(document, parsed = parseMakelist(document.getText())) {
  const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
  if (!workspaceFolder) {
    return [];
  }

  const calls = parsed.calls;
  const refs = [];

  for (const call of calls) {
    if (!PATH_MACROS.has(call.name)) {
      continue;
    }

    if (call.name === "INCLUDE" && call.args.length === 0) {
      refs.push({
        macro: call.name,
        range: new vscode.Range(document.positionAt(call.nameStart), document.positionAt(call.nameEnd)),
        unresolvedReason: "INCLUDE has no path argument.",
      });
      continue;
    }

    const args = call.name === "INCLUDE" ? call.args.slice(0, 1) : filterPathArguments(call.name, call.args);
    for (const arg of args) {
      const resolved = resolveMacroPath(document, workspaceFolder.uri, call.name, arg.value);
      if (resolved.skip) {
        continue;
      }

      refs.push({
        macro: call.name,
        kind: resolved.kind,
        range: new vscode.Range(document.positionAt(arg.start), document.positionAt(arg.end)),
        target: resolved.uri,
        linkTarget: resolved.linkTarget,
        displayTarget: resolved.displayTarget || arg.value,
        unresolvedReason: resolved.unresolvedReason,
      });
    }
  }

  return refs;
}

function findMacroAtPosition(document, position) {
  const offset = document.offsetAt(position);
  const parsed = parseMakelist(document.getText());

  for (const call of parsed.calls) {
    if (call.nameStart <= offset && offset <= call.nameEnd && MACRO_DOCS[call.name]) {
      return call.name;
    }
  }

  return undefined;
}

function findResourceRefAtPosition(document, position) {
  const offset = document.offsetAt(position);
  const parsed = parseMakelist(document.getText());

  for (const call of parsed.calls) {
    if (!RESOURCE_MACROS.has(call.name)) {
      continue;
    }
    for (const arg of call.args) {
      if (arg.start <= offset && offset <= arg.end && /^\d+$/.test(stripQuotes(arg.value))) {
        return {
          macro: call.name,
          value: stripQuotes(arg.value),
          range: new vscode.Range(document.positionAt(arg.start), document.positionAt(arg.end)),
        };
      }
    }
  }

  return undefined;
}

function formatMacroHover(macroName) {
  const doc = MACRO_DOCS[macroName];
  const markdown = new vscode.MarkdownString(undefined, true);
  markdown.appendCodeblock(doc.usage, "yamake");
  markdown.appendMarkdown(doc.text);
  return markdown;
}

function formatResourceHover(resourceId, mapping) {
  const markdown = new vscode.MarkdownString(undefined, true);
  markdown.appendMarkdown(`**Resource \`${resourceId}\`**\n\n`);
  markdown.appendMarkdown(`Declared in \`${mapping.relativePath}\`.\n\n`);
  if (mapping.description) {
    markdown.appendMarkdown(`${mapping.description}\n\n`);
  }
  markdown.appendMarkdown(mapping.url);
  return markdown;
}

async function findResourceMapping(document, resourceId) {
  const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
  if (!workspaceFolder) {
    return undefined;
  }

  const mappings = await loadResourceMappings(workspaceFolder.uri);
  return mappings.get(resourceId);
}

async function loadResourceMappings(workspaceUri) {
  const workspaceKey = workspaceUri.toString();
  if (resourceMappingCache && resourceMappingCache.workspaceKey === workspaceKey) {
    return resourceMappingCache.mappings;
  }

  const mappings = new Map();
  for (const relativePath of RESOURCE_MAPPING_FILES) {
    const uri = joinUri(workspaceUri, relativePath);
    let text;
    try {
      const bytes = await vscode.workspace.fs.readFile(uri);
      text = Buffer.from(bytes).toString("utf8");
    } catch (error) {
      continue;
    }

    let data;
    try {
      data = JSON.parse(text);
    } catch (error) {
      continue;
    }

    const resources = data.resources || {};
    const descriptions = data.resources_descriptions || {};
    for (const [id, url] of Object.entries(resources)) {
      if (mappings.has(id)) {
        continue;
      }
      const keyOffset = text.indexOf(`"${id}"`);
      const start = keyOffset >= 0 ? keyOffset + 1 : 0;
      const end = keyOffset >= 0 ? start + id.length : 0;
      mappings.set(id, {
        id,
        url,
        description: descriptions[id],
        relativePath,
        uri,
        location: new vscode.Location(uri, new vscode.Range(positionAtOffset(text, start), positionAtOffset(text, end))),
      });
    }
  }

  resourceMappingCache = { workspaceKey, mappings };
  return mappings;
}

function positionAtOffset(text, offset) {
  let line = 0;
  let lastLineStart = 0;
  for (let i = 0; i < offset; i += 1) {
    if (text[i] === "\n") {
      line += 1;
      lastLineStart = i + 1;
    }
  }
  return new vscode.Position(line, offset - lastLineStart);
}

function resolveMacroPath(document, workspaceUri, macro, rawValue) {
  const value = stripQuotes(rawValue);
  const kind = MACRO_KIND[macro];
  if (!value) {
    return { skip: true };
  }

  if (macro === "DATA" && UNSUPPORTED_DATA_PREFIXES.some((prefix) => value.startsWith(prefix))) {
    return { skip: true };
  }

  if (value.includes("$") && !value.startsWith("${ARCADIA_ROOT}/") && !value.startsWith("${CURDIR}/")) {
    return {
      unresolvedReason: `Cannot resolve path with unsupported variable: ${value}`,
    };
  }

  if (macro === "DATA") {
    if (value.startsWith("arcadia/")) {
      return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
    }
    if (value.startsWith("${ARCADIA_ROOT}/")) {
      return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
    }
    return {
      unresolvedReason: `DATA path is not a supported local Arcadia path: ${value}`,
    };
  }

  if (kind === "module-directory-root-relative") {
    if (value.startsWith("${ARCADIA_ROOT}/")) {
      return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
    }
    if (value.startsWith("arcadia/")) {
      return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
    }
    return fromWorkspace(workspaceUri, value, kind);
  }

  if (kind === "module-directory-current-relative") {
    if (value.startsWith("${ARCADIA_ROOT}/")) {
      return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
    }
    if (value.startsWith("arcadia/")) {
      return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
    }
    if (value.startsWith("${CURDIR}/")) {
      return fromDocumentDir(document.uri, value.slice("${CURDIR}/".length), kind);
    }
    return fromDocumentDir(document.uri, value, kind);
  }

  if (value.startsWith("${ARCADIA_ROOT}/")) {
    return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
  }

  if (value.startsWith("${CURDIR}/")) {
    return fromDocumentDir(document.uri, value.slice("${CURDIR}/".length), kind);
  }

  if (path.posix.isAbsolute(value)) {
    const uri = vscode.Uri.file(value);
    return { uri, linkTarget: linkTargetForKind(uri, kind), displayTarget: value, kind };
  }

  return fromDocumentDir(document.uri, value, kind);
}

function filterPathArguments(macro, args) {
  if (!MACRO_KIND[macro].startsWith("module-directory")) {
    return args;
  }

  return args.filter((arg) => {
    const value = stripQuotes(arg.value);
    return value !== "GLOBAL" && !value.startsWith("-") && !value.includes("=");
  });
}

function fromWorkspace(workspaceUri, relativePath, kind) {
  const normalized = normalizeRelativePath(relativePath);
  const uri = joinUri(workspaceUri, normalized);
  return {
    uri,
    linkTarget: linkTargetForKind(uri, kind),
    displayTarget: normalized,
    kind,
  };
}

function fromDocumentDir(documentUri, relativePath, kind) {
  const documentDir = documentUri.with({ path: path.posix.dirname(documentUri.path) });
  const normalized = normalizeRelativePath(relativePath);
  const uri = joinUri(documentDir, normalized);
  return {
    uri,
    linkTarget: linkTargetForKind(uri, kind),
    displayTarget: normalized,
    kind,
  };
}

function linkTargetForKind(uri, kind) {
  if (kind && kind.startsWith("module-directory")) {
    return vscode.Uri.joinPath(uri, "ya.make");
  }
  return uri;
}

function normalizeRelativePath(value) {
  return path.posix.normalize(value).replace(/^\/+/, "");
}

function joinUri(base, relativePath) {
  const parts = relativePath.split("/").filter(Boolean);
  return vscode.Uri.joinPath(base, ...parts);
}

function stripQuotes(value) {
  if (value.length >= 2) {
    const first = value[0];
    const last = value[value.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return value.slice(1, -1);
    }
  }
  return value;
}

function parseMakelist(text) {
  const calls = [];
  const errors = [];
  let i = 0;

  while (i < text.length) {
    const c = text[i];
    if (c === "#") {
      i = skipComment(text, i);
      continue;
    }

    if (!isIdentifierStart(c)) {
      i += 1;
      continue;
    }

    const nameStart = i;
    i += 1;
    while (i < text.length && isIdentifierPart(text[i])) {
      i += 1;
    }
    const nameEnd = i;
    const name = text.slice(nameStart, nameEnd).toUpperCase();

    i = skipWhitespace(text, i);
    if (text[i] !== "(") {
      continue;
    }

    const bodyStart = i + 1;
    const bodyEnd = findCallEnd(text, i);
    if (bodyEnd < 0) {
      errors.push({
        start: nameStart,
        end: Math.min(text.length, i + 1),
        message: `Unclosed ${name} macro call.`,
      });
      i += 1;
      continue;
    }

    calls.push({
      name,
      nameStart,
      nameEnd,
      args: parseArguments(text, bodyStart, bodyEnd),
    });
    i = bodyEnd + 1;
  }

  return { calls, errors };
}

function parseMacroCalls(text) {
  return parseMakelist(text).calls;
}

function findCallEnd(text, openParenOffset) {
  let depth = 1;
  let i = openParenOffset + 1;

  while (i < text.length) {
    const c = text[i];
    if (c === "#") {
      i = skipComment(text, i);
      continue;
    }
    if (c === '"' || c === "'") {
      i = skipQuoted(text, i);
      continue;
    }
    if (c === "\\") {
      i += 2;
      continue;
    }
    if (c === "(") {
      depth += 1;
    } else if (c === ")") {
      depth -= 1;
      if (depth === 0) {
        return i;
      }
    }
    i += 1;
  }

  return -1;
}

function parseArguments(text, start, end) {
  const args = [];
  let i = start;

  while (i < end) {
    i = skipWhitespace(text, i, end);
    if (i >= end) {
      break;
    }
    if (text[i] === "#") {
      i = skipComment(text, i);
      continue;
    }

    const argStart = i;
    if (text[i] === '"' || text[i] === "'") {
      i = Math.min(skipQuoted(text, i), end);
    } else {
      while (i < end && !isWhitespace(text[i]) && text[i] !== "#") {
        if (text[i] === "\\") {
          i += 2;
        } else {
          i += 1;
        }
      }
    }

    if (argStart < i) {
      args.push({
        value: text.slice(argStart, i),
        start: argStart,
        end: i,
      });
    }
  }

  return args;
}

function skipWhitespace(text, offset, limit = text.length) {
  let i = offset;
  while (i < limit && isWhitespace(text[i])) {
    i += 1;
  }
  return i;
}

function skipComment(text, offset) {
  let i = offset;
  while (i < text.length && text[i] !== "\n") {
    i += 1;
  }
  return i;
}

function skipQuoted(text, offset) {
  const quote = text[offset];
  let i = offset + 1;
  while (i < text.length) {
    if (text[i] === "\\") {
      i += 2;
      continue;
    }
    if (text[i] === quote) {
      return i + 1;
    }
    i += 1;
  }
  return i;
}

function isWhitespace(c) {
  return c === " " || c === "\t" || c === "\n" || c === "\r";
}

function isIdentifierStart(c) {
  return Boolean(c && /[A-Za-z_]/.test(c));
}

function isIdentifierPart(c) {
  return Boolean(c && /[A-Za-z0-9_-]/.test(c));
}

module.exports = {
  activate,
  deactivate,
};
