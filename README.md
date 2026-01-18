# DataChain

Build data analysis agents

Overview
--------

DataChain provides a small semantic modeling, BI-query DSL, validator, planner, and SQL AST/compiler foundation intended to power data-analysis agents. The library separates concerns into:
- a semantic layer (tables, columns, relationships, KPIs and filters),
- a BI-facing query DSL (measures, dimensions, filters, time grain, ordering),
- validators that check BI queries against the semantic model,
- a planner that transforms validated BI queries into a structured SQL AST,
- compilers that render the SQL AST into executable SQL for a target engine.

What's in the repository
------------------------

Key source files (under `src/datachain`):

- `src/datachain/__init__.py`: package initializer.

- `src/datachain/query/__init__.py`: query package initializer and top-level exports for the query subpackage.

- `src/datachain/query/models/__init__.py`: re-exports model modules and ensures Pydantic models are available.

- `src/datachain/query/models/enums.py`: enumerations used across the models such as `Sorting`, `Comparator`, `Aggregation`, and `Arithmetic`.

- `src/datachain/query/models/bi.py`: the BI-facing query DSL types and validations (`BIQuery`, `BIDimension`, `BIMeasure`, `BIFilter`, `BIOrderBy`, time-grain/window types). This file defines how callers describe analytic requests.

- `src/datachain/query/models/semantic.py`: the semantic modeling layer including `Table`, `SemanticColumn`, `Relationship`, `KPI`, `Filter`, and `SemanticModel`. It validates references, ensures relationships are well-formed, and exposes helpers for graph traversal and KPI/filter lookup.

- `src/datachain/query/models/sql.py`: a structured SQL AST for representing queries produced by the planner (e.g., `SQLQuery`, `SelectItem`, `QueryColumn`, `SQLMeasure`, `Join`, and logical predicates). Intended as the planner-to-compiler format.

- `src/datachain/query/validators.py`: validators that check a `BIQuery` against a `SemanticModel`. Responsibilities include verifying fields exist, checking time-grain compatibility, and resolving join/connectivity constraints.

- `src/datachain/query/planner.py`: transforms validated BI queries into the `SQLQuery` AST. This includes selecting measures/dimensions, injecting joins, and producing grouping/ordering structures.

- `src/datachain/query/compilers.py`: compiles the SQL AST into a target SQL dialect string (file exists; implementations for specific engines can be added here).

Tests
-----

Unit tests live under `tests/test_query` and exercise the BI models, semantic model, planner, and validators.

What the project is trying to do
--------------------------------

- Provide a clear semantic layer so business logic (KPIs, filters, table relationships) is defined once and reused.
- Offer a BI-friendly DSL so higher-level consumers can request metrics and slices without writing SQL.
- Validate BI requests against the semantic model to prevent invalid queries and to automatically infer joins and groupings.
- Plan and produce a structured SQL AST from validated BI queries so compilers can target different SQL engines safely.
- Enable data-analysis agents (or other orchestration layers) to convert natural-language or programmatic analysis requests into executable SQL using the stack above.

Next steps (suggested)
----------------------

- implement or extend compilers in `src/datachain/query/compilers.py` (e.g., DuckDB/Postgres dialects),
- improve planner join-resolution and complex KPI compilation in `src/datachain/query/planner.py`,
- extend validators to handle more advanced multi-hop join logic and time-window validations,
- add example notebooks or an examples/ folder demonstrating end-to-end usage.



