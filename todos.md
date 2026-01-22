# Finish
**Windows will create two SQL Queries** (Hahaha this is right)
1. review some sql
2. We need to add a window measure to the SQLQuery model for BIQueries with a window - rule is if window then create a CTE (windows to be captured in SQL measure)
    - I need to review some SQL to work this out properly
3. Planning (BIQuery -> SQLQuery)

** STOP **
Evaluate your code so far. Refactor to simplify, break out large functions into smaller ones, make it understandable 
!!!!!!!!!

# SQL Compiling
1. write the compiler base class (each sql language needs its own compiler) this is SQLQuery -> str
2. write the first concrete compiler class for duckdb

# Data Connection
- to execute queries
- doesn't this mean that a connection and a compiler should be combined

## Other considerations
- There will be shared logic between compilers (do this later)

# Agent
- then we move on to defining the agent class structure this will have stuff like register tools, it will need to be initialised with a semantic model etc...
- this is a package some base class (maybe), the planner agent (stateful), query executor (and other tool based stateless agents)

# Tools
- then we start defining tools starting with get_data
- DisplayChart (given what is returned by get_data (1 date, 1 numeric etc...) we have a list of available chart options. If there is one we use this if there is more we let the LLM select (or perhaps we just select based on a ranking the LLM just decides when to display a chart))  


# Write evals 
- create golden dataset
- set up each eval structure
