const vscode = require("vscode");

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
      bodyStart,
      bodyEnd,
      end: bodyEnd + 1,
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
  parseMakelist,
  positionAtOffset,
  stripQuotes,
};
