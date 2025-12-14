#12/08

I have better understood joins and the use of partition and, row number and lag. Joins Inner - all rows that are common Left - all rows that are common + rows from left table (null for right table values) Right - all rows that are common + rows from right table (null for left table values) full - all rows that are there (common or otherwise. nulls are placed when a value is not present) Row number assigns unique sequential numbers based on the specified order Partition by helps partition/segregate/group based on a column like customer_id and then we can order the rows within said partition by using an order by Lag is used to compare the data between a past point and a present. It can be a comparison between months or past orders. Both ROW_NUMBER and LAG use the same syntax of OVER (PARTITION BY c1 ORDER BY c2) Cortex AI is an AI service by Snowflake that helps deliver faster insights. We have currently staged the pdfs in a snowflake stage, parsed them into structured data (content and metadata). We are then Chunking the document to improve search and efficiency [maybe like splitting into smaller buckets] We are using Cortex.parse_document and Cortex.split_text_recursive_charachterparse_document which I presume are built in snowflake. Is it like a library in python where we use say pandas.xyz to use a function directly? Still a little unclear. The difference:
Python libraries run inside your application runtime
Cortex functions run inside Snowflake’s managed services layer
So conceptually:
Cortex is Snowflake’s built-in AI/ML function library, exposed through SQL.
What I did despite fear:
- showed up
- learned something new
- didn’t quit


#12/09
SQL summary Group by is used for aggregating based on a particular column. When we use count or sum or avg we can then use it. Having helps filter multiple rows unlike where which filters non aggregated rows. A case statement can be used within a select with case, when then else and end as. Common table expressions are like temporary tables that can be used to help a main query. They begin with a 'WITH' and have to be followed by a query that actually calls it. They don't require explicit permissions like views and are also not stored in the db. Cortex search can be created on chunks along with attributes. CORTEX.COMPLETE prompts are used to find key values that are stored as attributes. We then created a Cortex search service on top of it. I don't think there is any section to query the search service. The ipynb moves to a different section. I think a streamlit app is to be used here. Not sure. Also with regards to git - yesterday I downloaded git bash and have played around with the git commands. I also gave chapter quizes on w3schools and cleared them. I have reached the part of pushing code but since I have not linked my git bash to any actual repo I cannot push code anywhere. I was able to have branches and switch between branches.
What I did despite fear:
- showed up
- learned something new
- didn’t quit

#12/10
Virtual Warehouses are scalable computes used to process the data in snowflake. (Virtual Warehouses are Snowflake’s compute clusters responsible for executing queries, performing transformations, and processing data. They scale independently, pause when idle, and do not store data.)
Snowflake stores data in columnar, immutable micro partitions of 16mb sizes on an avg. This is never updated and upon a change in data a new micro partition is created. This helps in aggregations, filtering and also enables features like time travel and zero copy cloning. 
Snowflake already has micro partitions with min and max order dates. It is able to easily choose a micro partition and filter the data. 
One can run a statement to get the data from before this statement was run. One just needs the UUID of the statement itself. One can also restore data from a particular point in time. This makes restoring data easier. However, the default period of time that this can be used is 1 day. So one must use this feature as soon as possible. It can be set to a max of 90 days in Enterprise and Business Critical Editions.
Zero copy cloning allows us to make clones of databases, schemas and tables without using any extra storage. The storage is consumed only if the original or the clone are modified in any way
I have also learnt about Common table expressions and using them in tandum with windows functions. I am still not that good at analyzing the problems and formulating the correct SQL response, but I want to start thinking along those lines. 
I was also able to link my github, gitbash and vs code. I created a SSH key. I pushed code as well. 
What I did despite fear:
- showed up
- learned something new
- didn’t quit


#12/11
I need to get my confidence back. I need to not think of catastrophic situations. Take deep breaths. Have faith. I am compiling all the notes from the last few days. I pulled the git repo and cut a new branch and will push this note. Hopefully, I will keep updating this every day and track my progress. I'll trust the process. 
What I did despite fear:
- showed up
- learned something new
- didn’t quit

12/13
Snowflake Intelligence SI is used to answer complex questions to all the data in Snowflake -Deep Analysis Quick Actions - It goes beyond the what of the data to the why and takes in natural language and returns meaningful insights. -Verified Trusted Answers - You can trace a response back to the source -Enterprise Ready- Scalable SI and is within the perimeter of snowflake and is governed by the same security policies (**Snowflake Intelligence is an enterprise AI layer that enables governed, explainable, natural-language insights across all data (structured + unstructured) stored in Snowflake.**)

Agents return Insights based on the Retrievals (the better the context the stronger the insight) using a Large Language Model (LLM) 

Cortex Analyst is used to query structured data in Snowflake. A yaml file is used to provide sematic views that bridge the gap between business logic and actual snowflake table and columns. One can give alternate column names, sample values and even relationships between the said tables. **(Cortex Analyst converts natural-language questions into trusted SQL, using a semantic model (YAML) that defines metrics, dimensions, relationships, synonyms, and business logic**.)

Cortex Search is used to search and retrieve information from unstructured text data like slack messages and pdfs. It converts the data into chunks which in turn allow the agent to perform Retrieval Augmented Generation (RAG) 

Agent is used to answer questions (never just answer questions) or perform tasks for the users. It uses tools like Cortex analyst and cortex search and can have orchestration logic to perform actions like getting insights and sending out an email

**Cortex Agent is an orchestration layer that performs tasks for users by coordinating multiple tools.**
It can call Cortex Analyst to query structured data, Cortex Search to retrieve context from unstructured documents using RAG, and execute SQL or other actions.
Analyst itself is not an agent — it is one of the tools an agent can invoke to translate natural language into governed SQL.
In our POC, Cortex Search was used to retrieve insights from unstructured PDFs, which the agent could then incorporate into responses.
