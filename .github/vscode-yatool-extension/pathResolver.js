const path = require("path");
const vscode = require("vscode");

const {
  DIRECTORY_ARG_KEYWORDS,
  DIRECTORY_ARG_KEYWORDS_WITH_VALUE,
  MACRO_KIND,
  MODULE_DECLARATION_MACROS,
  PATH_MACROS,
  SOURCE_ARG_KEYWORDS,
  SOURCE_ARG_KEYWORDS_WITH_VALUE,
  SOURCE_FILE_MACROS,
  UNSUPPORTED_DATA_PREFIXES,
} = require("./macroMetadata");
const { parseMakelist, positionAtOffset, stripQuotes } = require("./makelistParser");

const MODULE_REFERENCE_MODIFIERS = new Set(["ADDINCL", "GLOBAL"]);
const RESOURCE_ARG_KEYWORDS = new Set(["DONT_PARSE", "FORCE_TEXT"]);
const GENERATED_OUTPUT_KEYWORDS = new Set(["OUT", "OUT_NOAUTO"]);
const RUN_MACRO_ARG_KEYWORDS = new Set([
  "CWD",
  "ENV",
  "IN",
  "IN_NOAUTO",
  "IN_NOPARSE",
  "OUT",
  "OUT_NOAUTO",
  "STDERR",
  "STDOUT",
  "TOOL",
]);

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
  const sourceRoots = collectSourceRoots(document, workspaceFolder.uri, parsed);
  const generatedOutputs = collectGeneratedOutputs(parsed);
  let conditionalDepth = 0;

  for (const call of calls) {
    if (call.name === "ENDIF") {
      conditionalDepth = Math.max(0, conditionalDepth - 1);
      continue;
    }

    const inConditional = conditionalDepth > 0;

    if (call.name === "IF") {
      conditionalDepth += 1;
      continue;
    }

    if (call.name === "ELSE" || call.name === "ELSEIF") {
      continue;
    }

    if (!PATH_MACROS.has(call.name)) {
      continue;
    }

    if (call.name === "INCLUDE" && call.args.length === 0) {
      refs.push({
        macro: call.name,
        range: new vscode.Range(document.positionAt(call.nameStart), document.positionAt(call.nameEnd)),
        inConditional,
        unresolvedReason: "INCLUDE has no path argument.",
      });
      continue;
    }

    const args = pathArgumentsForCall(call);
    for (const arg of args) {
      const resolved = resolveMacroPath(document, workspaceFolder.uri, call.name, arg.value, { sourceRoots });
      if (resolved.skip) {
        continue;
      }

      refs.push({
        macro: call.name,
        kind: resolved.kind,
        range: new vscode.Range(document.positionAt(arg.start), document.positionAt(arg.end)),
        target: resolved.uri,
        candidateTargets: resolved.candidateTargets,
        linkTarget: resolved.linkTarget,
        displayTarget: resolved.displayTarget || arg.value,
        generatedOutput: generatedOutputs.has(stripQuotes(arg.value)),
        inConditional,
        unresolvedReason: resolved.unresolvedReason,
      });
    }
  }

  return refs;
}

function findPathRefAtPosition(document, position) {
  const refs = collectPathRefs(document);
  return refs.find((ref) => ref.range && ref.range.contains(position) && ref.target && !ref.unresolvedReason);
}

function resolveMacroPath(document, workspaceUri, macro, rawValue, context = {}) {
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

  if (isSourceLikeFileKind(kind)) {
    return resolveSourceFilePath(document, workspaceUri, value, context.sourceRoots || [], kind);
  }

  if (kind === "directory") {
    return resolveDirectoryPath(document, workspaceUri, value);
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

  if (kind === "module-directory-root-relative" || kind === "module-reference-root-relative") {
    if (value.startsWith("${ARCADIA_ROOT}/")) {
      return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
    }
    if (value.startsWith("arcadia/")) {
      return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
    }
    return fromWorkspace(workspaceUri, value, kind);
  }

  if (kind === "module-directory-current-relative" || kind === "module-reference-current-relative") {
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
  if (!isModuleLikeKind(MACRO_KIND[macro])) {
    return args;
  }

  return args.filter((arg) => {
    const value = stripQuotes(arg.value);
    return !MODULE_REFERENCE_MODIFIERS.has(value.toUpperCase()) && !value.startsWith("-") && !value.includes("=");
  });
}

function pathArgumentsForCall(call) {
  if (call.name === "INCLUDE" || call.name === "USE_RECIPE") {
    return call.args.slice(0, 1);
  }
  if (call.name === "ADDINCL") {
    return filterDirectoryArguments(call.args);
  }
  if (call.name === "ARCHIVE") {
    return filterArchiveFileArguments(call.args);
  }
  if (call.name === "RESOURCE") {
    return filterResourceFileArguments(call.args);
  }
  if (call.name === "JOIN_SRCS") {
    return filterSourceFileArguments(call.args.slice(1));
  }
  if (call.name === "RUN_PYTHON3") {
    return filterSourceFileArguments(call.args.slice(0, 1));
  }
  if (SOURCE_FILE_MACROS.has(call.name)) {
    return filterSourceFileArguments(call.args);
  }
  return filterPathArguments(call.name, call.args);
}

function filterSourceFileArguments(args) {
  const result = [];
  let skipNext = 0;

  for (const arg of args) {
    if (skipNext > 0) {
      skipNext -= 1;
      continue;
    }

    const value = stripQuotes(arg.value);
    if (!value || value.startsWith("-") || value.includes("=") || value.includes("*")) {
      continue;
    }
    const keyword = value.toUpperCase();
    if (SOURCE_ARG_KEYWORDS_WITH_VALUE.has(keyword)) {
      skipNext = 1;
      continue;
    }
    if (SOURCE_ARG_KEYWORDS.has(keyword)) {
      continue;
    }
    result.push(arg);
  }

  return result;
}

function filterDirectoryArguments(args) {
  const result = [];
  let skipNext = 0;

  for (const arg of args) {
    if (skipNext > 0) {
      skipNext -= 1;
      continue;
    }

    const value = stripQuotes(arg.value);
    if (!value || value.startsWith("-") || value.includes("=")) {
      continue;
    }
    const keyword = value.toUpperCase();
    if (DIRECTORY_ARG_KEYWORDS_WITH_VALUE.has(keyword)) {
      skipNext = 1;
      continue;
    }
    if (DIRECTORY_ARG_KEYWORDS.has(keyword)) {
      continue;
    }
    result.push(arg);
  }

  return result;
}

function filterResourceFileArguments(args) {
  const result = [];
  let expectingSource = true;

  for (const arg of args) {
    const value = stripQuotes(arg.value);
    const keyword = value.toUpperCase();
    if (!value || RESOURCE_ARG_KEYWORDS.has(keyword)) {
      continue;
    }
    if (value === "-" || value.startsWith("-") || value.includes("=")) {
      expectingSource = false;
      continue;
    }
    if (expectingSource) {
      result.push(arg);
    }
    expectingSource = !expectingSource;
  }

  return result;
}

function collectGeneratedOutputs(parsed) {
  const outputs = new Set();

  for (const call of parsed.calls) {
    let collecting = false;
    for (const arg of call.args) {
      const value = stripQuotes(arg.value);
      const keyword = value.toUpperCase();
      if (GENERATED_OUTPUT_KEYWORDS.has(keyword)) {
        collecting = true;
        continue;
      }
      if (collecting && RUN_MACRO_ARG_KEYWORDS.has(keyword)) {
        collecting = GENERATED_OUTPUT_KEYWORDS.has(keyword);
        continue;
      }
      if (!collecting || !value || value.startsWith("-") || value.includes("=") || value.includes("$")) {
        continue;
      }
      outputs.add(value);
    }
  }

  return outputs;
}

function filterArchiveFileArguments(args) {
  const result = [];
  let skipNext = 0;

  for (const arg of args) {
    if (skipNext > 0) {
      skipNext -= 1;
      continue;
    }

    const value = stripQuotes(arg.value);
    const keyword = value.toUpperCase();
    if (!value || keyword === "DONT_COMPRESS") {
      continue;
    }
    if (keyword === "NAME") {
      skipNext = 1;
      continue;
    }
    result.push(arg);
  }

  return filterSourceFileArguments(result);
}

function resolveSourceFilePath(document, workspaceUri, value, sourceRoots, kind = "source-file") {
  if (value.startsWith("${ARCADIA_ROOT}/")) {
    return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
  }

  if (value.startsWith("arcadia/")) {
    return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
  }

  if (value.startsWith("${CURDIR}/")) {
    return fromDocumentDir(document.uri, value.slice("${CURDIR}/".length), kind);
  }

  if (path.posix.isAbsolute(value)) {
    const uri = vscode.Uri.file(value);
    return { uri, candidateTargets: [uri], linkTarget: uri, displayTarget: value, kind };
  }

  const normalized = normalizeRelativePath(value);
  const candidates = uniqueUris([
    joinUri(document.uri.with({ path: path.posix.dirname(document.uri.path) }), normalized),
    ...sourceRoots.map((root) => joinUri(root, normalized)),
    joinUri(workspaceUri, normalized),
  ]);

  return {
    uri: candidates[0],
    candidateTargets: candidates,
    linkTarget: candidates[0],
    displayTarget: normalized,
    kind,
  };
}

function resolveDirectoryPath(document, workspaceUri, value) {
  const kind = "directory";

  if (value.startsWith("${ARCADIA_ROOT}/")) {
    return fromWorkspace(workspaceUri, value.slice("${ARCADIA_ROOT}/".length), kind);
  }

  if (value.startsWith("arcadia/")) {
    return fromWorkspace(workspaceUri, value.slice("arcadia/".length), kind);
  }

  if (value.startsWith("${CURDIR}/")) {
    return fromDocumentDir(document.uri, value.slice("${CURDIR}/".length), kind);
  }

  if (path.posix.isAbsolute(value)) {
    const uri = vscode.Uri.file(value);
    return { uri, candidateTargets: [uri], linkTarget: uri, displayTarget: value, kind };
  }

  const normalized = normalizeRelativePath(value);
  const candidates = uniqueUris([
    joinUri(document.uri.with({ path: path.posix.dirname(document.uri.path) }), normalized),
    joinUri(workspaceUri, normalized),
  ]);

  return {
    uri: candidates[0],
    candidateTargets: candidates,
    linkTarget: candidates[0],
    displayTarget: normalized,
    kind,
  };
}

function collectSourceRoots(document, workspaceUri, parsed) {
  const roots = [];

  for (const call of parsed.calls) {
    if ((call.name === "UNITTEST_FOR" || call.name === "JTEST_FOR" || call.name === "GO_TEST_FOR") && call.args.length > 0) {
      const uri = resolveSourceRootPath(document, workspaceUri, call.args[0].value);
      if (uri) {
        roots.push(uri);
      }
      continue;
    }

    if (call.name === "SRCDIR") {
      for (const arg of call.args) {
        const uri = resolveSourceRootPath(document, workspaceUri, arg.value);
        if (uri) {
          roots.push(uri);
        }
      }
    }
  }

  return uniqueUris(roots);
}

function resolveSourceRootPath(document, workspaceUri, rawValue) {
  const value = stripQuotes(rawValue);
  if (!value || value.includes("$") && !value.startsWith("${ARCADIA_ROOT}/") && !value.startsWith("${CURDIR}/")) {
    return undefined;
  }
  if (value.startsWith("${ARCADIA_ROOT}/")) {
    return joinUri(workspaceUri, normalizeRelativePath(value.slice("${ARCADIA_ROOT}/".length)));
  }
  if (value.startsWith("arcadia/")) {
    return joinUri(workspaceUri, normalizeRelativePath(value.slice("arcadia/".length)));
  }
  if (value.startsWith("${CURDIR}/")) {
    const documentDir = document.uri.with({ path: path.posix.dirname(document.uri.path) });
    return joinUri(documentDir, normalizeRelativePath(value.slice("${CURDIR}/".length)));
  }
  if (path.posix.isAbsolute(value)) {
    return vscode.Uri.file(value);
  }
  return joinUri(workspaceUri, normalizeRelativePath(value));
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

async function resolveLinkTarget(ref, workspaceUri) {
  if (isSourceLikeFileKind(ref.kind)) {
    const target = await findExistingFileUri(ref.candidateTargets || [ref.target]);
    return target || ref.linkTarget || ref.target;
  }

  if (isPlainDirectoryKind(ref.kind)) {
    const target = await findExistingDirectoryUri(ref.candidateTargets || [ref.target]);
    return target || ref.linkTarget || ref.target;
  }

  if (!isModuleLikeKind(ref.kind)) {
    return ref.linkTarget || ref.target;
  }

  if (isModuleReferenceKind(ref.kind)) {
    const owner = await findModuleReferenceTarget(ref.target, workspaceUri);
    return owner || ref.linkTarget || ref.target;
  }

  const owner = await findOwnerYaMake(ref.target, workspaceUri);
  return owner || ref.linkTarget || ref.target;
}

async function resolveRefLocation(ref, workspaceUri) {
  if (isSourceLikeFileKind(ref.kind)) {
    const target = await findExistingFileUri(ref.candidateTargets || [ref.target]);
    return target ? new vscode.Location(target, new vscode.Position(0, 0)) : undefined;
  }

  if (isPlainDirectoryKind(ref.kind)) {
    const target = await findExistingDirectoryUri(ref.candidateTargets || [ref.target]);
    return target ? new vscode.Location(target, new vscode.Position(0, 0)) : undefined;
  }

  if (isModuleReferenceKind(ref.kind)) {
    return findModuleReferenceLocation(ref.target, workspaceUri);
  }

  if (isModuleDirectoryKind(ref.kind)) {
    const owner = await findOwnerYaMake(ref.target, workspaceUri);
    return owner ? new vscode.Location(owner, new vscode.Position(0, 0)) : undefined;
  }

  return ref.target ? new vscode.Location(ref.target, new vscode.Position(0, 0)) : undefined;
}

async function findModuleReferenceTarget(uri, workspaceUri) {
  const location = await findModuleReferenceLocation(uri, workspaceUri);
  return location && location.uri;
}

async function findModuleReferenceLocation(uri, workspaceUri) {
  const moduleName = path.posix.basename(uri.path);
  let current = uri;

  while (true) {
    for (const filename of ["ya.make.inc", "ya.make"]) {
      const candidate = vscode.Uri.joinPath(current, filename);
      const location = await findModuleDeclarationLocation(candidate, moduleName);
      if (location) {
        return location;
      }
    }

    const owner = await findOwnerYaMake(current, current);
    if (owner) {
      return new vscode.Location(owner, new vscode.Position(0, 0));
    }

    const parentPath = path.posix.dirname(current.path);
    if (parentPath === current.path || (workspaceUri && current.path === workspaceUri.path)) {
      return undefined;
    }
    current = current.with({ path: parentPath });
  }
}

async function findModuleDeclarationLocation(uri, moduleName) {
  try {
    const bytes = await vscode.workspace.fs.readFile(uri);
    const text = Buffer.from(bytes).toString("utf8");
    const parsed = parseMakelist(text);
    const call = parsed.calls.find((candidate) => {
      if (!MODULE_DECLARATION_MACROS.has(candidate.name)) {
        return false;
      }
      if (candidate.args.length === 0) {
        return candidate.name === "PACKAGE";
      }
      return stripQuotes(candidate.args[0].value) === moduleName;
    });
    if (!call) {
      return undefined;
    }
    const target = call.args[0] || { start: call.nameStart, end: call.nameEnd };
    return new vscode.Location(uri, new vscode.Range(positionAtOffset(text, target.start), positionAtOffset(text, target.end)));
  } catch (error) {
    return undefined;
  }
}

async function findExistingFileUri(uris) {
  for (const uri of uris) {
    try {
      const stat = await vscode.workspace.fs.stat(uri);
      if (!isDirectory(stat)) {
        return uri;
      }
    } catch (error) {
      // Try the next candidate.
    }
  }

  return undefined;
}

async function findExistingDirectoryUri(uris) {
  for (const uri of uris) {
    try {
      const stat = await vscode.workspace.fs.stat(uri);
      if (isDirectory(stat)) {
        return uri;
      }
    } catch (error) {
      // Try the next candidate.
    }
  }

  return undefined;
}

function uniqueUris(uris) {
  const seen = new Set();
  const result = [];

  for (const uri of uris) {
    const key = uri.toString();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(uri);
  }

  return result;
}

function isSourceLikeFileKind(kind) {
  return kind === "source-file" || kind === "resource-file";
}

function isPlainDirectoryKind(kind) {
  return kind === "directory";
}

function isModuleLikeKind(kind) {
  return isModuleDirectoryKind(kind) || isModuleReferenceKind(kind);
}

function isModuleDirectoryKind(kind) {
  return Boolean(kind && kind.startsWith("module-directory"));
}

function isModuleReferenceKind(kind) {
  return Boolean(kind && kind.startsWith("module-reference"));
}

async function findOwnerYaMake(uri, workspaceUri) {
  let current = uri;

  while (true) {
    const candidate = vscode.Uri.joinPath(current, "ya.make");
    try {
      await vscode.workspace.fs.stat(candidate);
      return candidate;
    } catch (error) {
      // Keep walking up until the workspace root is reached.
    }

    const parentPath = path.posix.dirname(current.path);
    if (parentPath === current.path || (workspaceUri && current.path === workspaceUri.path)) {
      return undefined;
    }
    current = current.with({ path: parentPath });
  }
}

function normalizeRelativePath(value) {
  return path.posix.normalize(value).replace(/^\/+/, "");
}

function joinUri(base, relativePath) {
  const parts = relativePath.split("/").filter(Boolean);
  return vscode.Uri.joinPath(base, ...parts);
}

module.exports = {
  collectPathRefs,
  findExistingDirectoryUri,
  findExistingFileUri,
  findModuleReferenceTarget,
  findOwnerYaMake,
  findPathRefAtPosition,
  isDirectory,
  isModuleDirectoryKind,
  isModuleReferenceKind,
  isPlainDirectoryKind,
  isSourceLikeFileKind,
  resolveLinkTarget,
  resolveRefLocation,
};
