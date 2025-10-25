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
- [x] Extend CREATE SCHEMA grammar with `AUTHORIZATION` and embedded statement support.
- [x] Parse CREATE SCHEMA embedded statements into dedicated AST nodes rather than raw strings.
- [x] Extend DROP SCHEMA grammar with `RESTRICT` handling and multiple schema targets.
- [x] Support per-schema `IF EXISTS` flags and richer diagnostic context for bulk drops.
- [x] Implement CREATE/DROP TABLE grammar with column definitions (type literals, `NOT NULL`, default expressions stubbed).
- [x] Extend CREATE TABLE grammar with column default expressions and inline constraint variants (PRIMARY KEY, UNIQUE).
- [x] Support complex column default expressions (function calls, arithmetic) and inline constraint naming.
- [x] Capture parser diagnostics for missing keywords and duplicate identifiers.
- [x] Extend diagnostics with remediation hints and statement context for complex scripts.
- [x] Translate parser diagnostics to `DdlCommandResponse` severity/hints (warning for user input issues, error for parser bugs).
- [x] Extend unit tests with positive/negative coverage for each grammar element.

## Milestone 2: Grammar Infrastructure Hardening (1 sprint)
- [x] Introduce error-recovery hooks (synchronise at statement terminators) to continue parsing multi-statement scripts.
- [x] Implement common expression grammar primitives (string literals, numeric literals) needed for defaults and expressions.
- [ ] Benchmark parser performance using representative scripts; set baseline targets for future regressions. _(Deferred to Milestone 3: integration benchmarking phase.)_
- [x] Document grammar extension guidelines (naming, action hooks, AST mapping) in `docs/parser_frontend_milestones.md` appendix.

## Milestone 3: Integration with Catalog Semantics (1 sprint)
- [x] Map AST nodes to semantic structures consumed by DDL dispatcher (request structs, options).  \
	Implemented `parser::build_ddl_commands`, which converts successful statement ASTs into `bored::ddl::DdlCommand` requests and propagates parser diagnostics.
- [ ] Plug parser diagnostics into telemetry (parser failure counters, average parse duration).
- [ ] Add integration tests that parse + dispatch CREATE TABLE / DROP TABLE flows end-to-end.
- [ ] Update operator documentation to describe parser error messages and remediation hints.

## Deliverables & Exit Criteria
- PEGTL dependency committed and verified via CI build.
- Parser unit test suite running under `ctest` with deterministic fixtures.
- Documentation updated (this doc plus references from `relational_layer_design.md`).
- Parser module capable of translating basic DDL SQL into existing dispatcher request structs with actionable diagnostics.

### Appendix: Grammar Extension Guidelines
- Keep new grammar rules in `src/parser/grammar.cpp` scoped to the smallest sensible region; prefer `namespace`-local structs when the rule is an implementation detail. Name rules using `<entity>_<role>_rule` to mirror existing patterns (`default_expression_rule`, `schema_embedded_statement_rule`).
- Use dedicated parse-state structs for per-statement context that carries mutable state between actions; avoid globals and reset state at rule boundaries (see `CreateTableParseState`).
- Implement PEGTL action specialisations only when a rule needs semantic side effects. Fall back to the default templated no-op action otherwise to keep compile times low and intent clear.
- Normalise parsed text through helpers such as `trim_copy` before storing it in AST nodes. This keeps downstream consumers from re-implementing whitespace stripping.
- Surface diagnostics for ambiguous or unsupported constructs immediately during parsing. Reuse `make_parse_error` for PEGTL exceptions and append custom diagnostics for logical validations (duplicate column names, duplicate schema targets).
- Extend the Catch2 suite alongside every grammar change. Add positive coverage to the relevant `parser_ddl_*` tests and negative coverage that exercises diagnostics; remember to update `CMakeLists.txt` if new test translation units are introduced.
