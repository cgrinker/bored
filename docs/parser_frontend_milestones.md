# Parser Front-End Milestones (PEGTL)

## Objectives
- Parse SQL DDL (initial scope) into an abstract syntax tree (AST) using [`taocpp::PEGTL`](https://github.com/taocpp/PEGTL).
- Surface parser diagnostics that map cleanly into `DdlCommandResponse` severity, error, and remediation hints.
- Provide reusable grammar modules (lexical rules, expressions, statements) to seed future DML parsing.
- Integrate the parser into the existing build with vcpkg-provided PEGTL and ensure CI covers parser unit tests.

## Milestone 0: PEGTL Integration & Scaffolding (0.5 sprint)
- [x] Add `taocpp-pegtl` to `vcpkg.json` (done) and ensure the root `CMakeLists.txt` links against `pegtl::pegtl`.
- [x] Create `src/parser` module with umbrella headers (`parser/grammar.hpp`, `parser/ast.hpp`).
- [x] Establish baseline identifier grammar (whitespace skipping, bare identifiers); track quoted identifier and keyword handling as follow-up.
- [x] Implement parser unit test harness using Catch2; include golden SQL snippets under `tests/parser_samples/`.
- [x] Wire parser build flag (`BORED_ENABLE_PARSER`) so downstream consumers can opt-in during bring-up.

## Milestone 1: DDL Statement Grammar (1 sprint)
- [x] Implement CREATE/DROP DATABASE grammar and AST nodes.
- [x] Implement CREATE/DROP SCHEMA grammar and AST nodes (including `IF NOT EXISTS` / `IF EXISTS`).
- [ ] Extend CREATE SCHEMA grammar with `AUTHORIZATION` and embedded statement support.
- [ ] Extend DROP SCHEMA grammar with `RESTRICT` handling and multiple schema targets.
- [x] Implement CREATE/DROP TABLE grammar with column definitions (type literals, `NOT NULL`, default expressions stubbed).
- [x] Extend CREATE TABLE grammar with column default expressions and inline constraint variants (PRIMARY KEY, UNIQUE).
- [ ] Support complex column default expressions (function calls, arithmetic) and inline constraint naming.
- [ ] Capture parser diagnostics: unexpected token, missing keywords, duplicate identifiers.
- [ ] Translate parser diagnostics to `DdlCommandResponse` severity/hints (warning for user input issues, error for parser bugs).
- [ ] Extend unit tests with positive/negative coverage for each grammar element.

## Milestone 2: Grammar Infrastructure Hardening (1 sprint)
- [ ] Introduce error-recovery hooks (synchronise at statement terminators) to continue parsing multi-statement scripts.
- [ ] Implement common expression grammar primitives (string literals, numeric literals) needed for defaults and expressions.
- [ ] Benchmark parser performance using representative scripts; set baseline targets for future regressions.
- [ ] Document grammar extension guidelines (naming, action hooks, AST mapping) in `docs/parser_frontend_milestones.md` appendix.

## Milestone 3: Integration with Catalog Semantics (1 sprint)
- [ ] Map AST nodes to semantic structures consumed by DDL dispatcher (request structs, options).
- [ ] Plug parser diagnostics into telemetry (parser failure counters, average parse duration).
- [ ] Add integration tests that parse + dispatch CREATE TABLE / DROP TABLE flows end-to-end.
- [ ] Update operator documentation to describe parser error messages and remediation hints.

## Deliverables & Exit Criteria
- PEGTL dependency committed and verified via CI build.
- Parser unit test suite running under `ctest` with deterministic fixtures.
- Documentation updated (this doc plus references from `relational_layer_design.md`).
- Parser module capable of translating basic DDL SQL into existing dispatcher request structs with actionable diagnostics.
