# IBIS

- I just found out about ibis - it does everything I need in a very elegant way - for sure this is the way to go
- I will only need my semantic models and query 
- There will still be a validator a resolver and a planner but the planner will look slightly different as it will map a biquery to an ibis expression
- Also for now lets remove the agents ability to write its own measure and metrics - everything should be defined by the user
- We will also need to define data connections and provide a wrapper around how ibis handles data connection

# Agent
- then we move on to defining the agent class structure this will have stuff like register tools, it will need to be initialised with a semantic model etc...
- this is a package some base class (maybe), the planner agent (stateful), query executor (and other tool based stateless agents)

# Tools
- then we start defining tools starting with get_data
- DisplayChart (given what is returned by get_data (1 date, 1 numeric etc...) we have a list of available chart options. If there is one we use this if there is more we let the LLM select (or perhaps we just select based on a ranking the LLM just decides when to display a chart))  


# Write evals 
- create golden dataset
- set up each eval structure


- How do we handle situations where we need to first filter a table and then calculate the results. 
- We need to handle filters seperately to select items and add the SQL so that we filter then aggregate
- We should capture this in the query context when we analyse the context