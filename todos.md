# Finish
**Windows will create two SQL Queries** (this is right)
1. From a resolved biquery we need to plan this sqlquery this means
    - Do we have a subquery to create? (yes if there is a window)
    - What is the common table and what are the joins (for this we need this list of tables in the resolved query)
    - What do we need to groupby - if subquery all groupbys go in the subquery and the window logic then we create an outer query that selects from this 


** STOP **
Evaluate your code so far. Refactor to simplify, break out large functions into smaller ones, make it understandable 
!!!!!!!!!

# SQL Compiling
1. write the compiler base class (each sql language needs its own compiler) this is SQLQuery -> str
2. write the first concrete compiler class for duckdb

# Data Connection
- to execute queries
- doesn't this mean that a connection and a compiler should be combined

# Agent
- then we move on to defining the agent class structure this will have stuff like register tools, it will need to be initialised with a semantic model etc...
- this is a package some base class (maybe), the planner agent (stateful), query executor (and other tool based stateless agents)

# Tools
- then we start defining tools starting with get_data
- DisplayChart (given what is returned by get_data (1 date, 1 numeric etc...) we have a list of available chart options. If there is one we use this if there is more we let the LLM select (or perhaps we just select based on a ranking the LLM just decides when to display a chart))  


# Write evals 
- create golden dataset
- set up each eval structure
