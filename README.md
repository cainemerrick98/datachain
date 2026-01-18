# DataChain

Build data analysis agents

Overview
--------

...

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

...

Next steps (suggested)
----------------------

...


