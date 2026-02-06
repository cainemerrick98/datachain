# V1
Its just a query executor with access to a semantic model it creates bi queries that are compiled to sql 

# Next
- Read code make todos on bits you dont like / want to change
- Plan out what needs to be tested
- Write the fixtures for the tests (most data can be reused across the tests)
- Write the tests

# SQL Compiling
1. write the compiler base class (each sql language needs its own compiler) this is SQLQuery -> str
2. write the first concrete compiler class for duckdb

# Data Connection
- to execute queries
- Start with a duckdb connection
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
