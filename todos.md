# SQL Compiling
1. Review the sql.py module do we have the finished model?
2. Write the function to convert BIQuery -> SQLQuery
3. write the compiler base class (each sql language needs its own compiler) this is SQLQuery -> str
4. write the first concrete compiler class for duckdb

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

# Write evals 
- create golden dataset
- set up each eval structure

# Further Additions
- Calculated Metrics e.g. revenue - costs
- Column to column comparsions/filters