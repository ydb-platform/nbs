# C++ section separators

These rules apply to C++ source and header files under `cloud/filestore`.
They describe the convention used in the existing code; preserve the local
grouping when editing a file.

## Format

Use a standalone line of exactly 80 forward slashes, starting at column 1:

```cpp
////////////////////////////////////////////////////////////////////////////////
```

Leave one blank line before and after it. Do not indent the separator or append
a section title to it. Put any explanatory comment on a separate line.

## Placement

- Put a separator before the first logical declaration or definition section
  inside a namespace, after any namespace-level `using` directives. If that
  section is in an anonymous namespace, put it inside that namespace. Some
  files also separate the anonymous namespace itself; follow the file's pattern.
- Separate distinct namespace-level sections: constants or lookup tables,
  forward declarations, helper functions, classes, and public implementations.
  Keep closely related declarations together rather than separating each one.
- Put a separator after an anonymous namespace closes when public definitions
  follow. The separator belongs before those definitions, not immediately before
  the namespace's closing brace.
- Separate distinct namespace-level class or struct definitions, and separate
  their out-of-line implementation groups. Keep related methods of the same
  class together; a large implementation may have several sections by purpose.
- In headers, use separators before logical groups of declarations, not before
  every declaration or class member.
- In tests, use separators before namespace-level fixtures/helper groups and
  test suites. Do not add one before every test within a suite.
- Use ordinary blank lines within function bodies and class definitions.
  Do not insert this separator between statements, access specifiers, or
  individual inline methods.
- Do not add a trailing separator before the final namespace close or at EOF.
  Do not use it to divide includes or replace namespace-closing comments.

## Editing existing code

When adding code, decide whether it extends an existing section or starts a
distinct one. Extend an existing section without adding a separator; add one
when introducing a new logical group. Preserve nearby separators and avoid
unrelated formatting changes. Do not infer a separator requirement from a
function's length alone.

## Examples from this tree

- `tools/analytics/profile_tool/lib/command.cpp`: the first separator introduces
  the private constant; the separator after the anonymous namespace introduces
  all `TCommand` implementations. There are no separators between those methods.
- `tools/analytics/profile_tool/lib/factory.cpp`: separate groups contain command
  forward declarations, factory aliases/table, and related public functions.
- `tools/analytics/profile_tool/lib/dump_events.cpp`: each private class starts
  a section, followed by a separate public factory section.
- `libs/storage/tablet/tablet_actor.cpp`: related in-flight request methods share
  a section; other method groups have their own separators.
- `tools/analytics/profile_tool/lib/factory_ut.cpp`: one separator introduces the
  suite; individual tests are separated by blank lines.
