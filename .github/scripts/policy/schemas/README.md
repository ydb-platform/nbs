# Policy JSON schemas

- `github-actions-policy.schema.json`: GitHub's request/response schemas and referenced components.
- `policy.schema.json`: structural restrictions for local authoring and simulation.
- `local-policy.schema.json`: create-request validation plus NBS-only restrictions.
- `live-policy.schema.json`: GitHub's full response validation, without local restrictions.

The Python loader only reads these files and builds an offline reference registry. The URN
identifiers are local schema identifiers, not URLs to fetch. Unknown references fail; schema
validation never downloads content. Files live here rather than in `.github/policy` so they
are not mistaken for importable policy bodies.

## Upstream source

API version: **2026-03-10**
Repository: [github/rest-api-description](https://github.com/github/rest-api-description)
Revision: **0029c0d4a6d330559abe0e45b22263bbe1c6b1a7**

Source: [versioned OpenAPI document](https://github.com/github/rest-api-description/blob/0029c0d4a6d330559abe0e45b22263bbe1c6b1a7/descriptions/api.github.com/api.github.com.2026-03-10.json).

The vendored subset contains:

- the request body for `POST /repos/{owner}/{repo}/actions/policies`;
- the 200 response for `GET /repos/{owner}/{repo}/actions/policies/{policy_id}`;
- their transitive `components.schemas` references.

The source uses OpenAPI 3.0.3, not standalone JSON Schema. For Draft 2020-12 validation,
component references were relocated from `#/components/schemas/` to `#/$defs/`, and
`nullable: true` was converted to an `anyOf` null alternative. Other constraints and
descriptions are retained. The upstream MIT license is included in `LICENSE.github`.
The response example in `../testdata/github-policy-response.json` is from the same revision.

When updating, extract the same subset at a reviewed revision, apply those two conversions,
retain the license, and update the provenance here and in the JSON file's `x-source`.
Do not replace our overlay files with upstream definitions.

## Why overlays remain

GitHub allows omitted conditions/rules, wildcard scopes, and Enterprise-only `evaluate`.
Its response schema also requires server fields (`id`, `target`, `source_type`, `source`)
that do not belong in importable request bodies.

Local files require explicit workflow targeting and rule parameters, exact paths or `~ALL`,
exact-path exclusions, `NBS: ` names, `active`/`disabled`, and no unknown fields.
Actor and event enums come from GitHub's schema, not separate Python lists. The two supported rule
kinds remain explicit in our overlay because adding a GitHub rule does not implement its simulation.

Live parsing accepts omitted/null conditions, removed rules, empty allowlists, and duplicate
rule/actor entries so legitimate changes can be reported by the drift checker. It normalizes
omitted conditions and empty includes to `~ALL`; these responses are not used for simulation.

Python retains filesystem, name-uniqueness, and catch-all exception-coverage checks. Overlaps
are allowed: the simulator applies every matching policy. Its validator also rejects float/bool
IDs, preserving strict JSON integer IDs.

Run from the repository root:

```bash
PYTHONPATH=.github pytest -q .github/scripts/policy
PYTHONPATH=.github python3 -m scripts.policy.validate
```
