# Exercise 1: Building a Property Analytics POC with Cortex Analyst

## Objective

Build a working real estate portfolio analytics demo using **Cortex Analyst** and **Snowflake Intelligence** that lets non-technical stakeholders ask questions about property transactions, agent performance, and market trends in plain English.

**Time:** ~60 minutes
**Deliverables:**
- Real estate demo database with realistic property, agent, and transaction data
- Semantic view for Cortex Analyst
- Cortex Analyst responding correctly to natural language queries

---

## Scenario

You're a Data Analyst at **Apex Property Group**, a commercial and residential brokerage operating across five major US markets. Your Head of Operations, Nina Okafor, has challenged you to show whether Snowflake's AI capabilities can replace the manual Excel reporting the leadership team currently depends on.

> *"Our agents close deals on Friday and the directors are still waiting for numbers on Tuesday. I want to ask questions and get answers — not wait for a report."*
> — Nina Okafor, Head of Operations

Your job: build a proof-of-concept that demonstrates Cortex Analyst answering real business questions about property transactions and operating expenses — without anyone writing SQL.

---

## Prerequisites

- Active Snowflake account (trial or enterprise)
- SYSADMIN or equivalent role with CREATE DATABASE privileges
- Snowflake Worksheet or compatible SQL editor
- `setup.sql` from this folder

---

## Before You Start — Set Your Initials

Open `setup.sql` and find-replace `_ABC` with your own initials before running anything.

**Example:**
| Your Name | Replace `_ABC` with |
|-----------|-------------------|
| Jane Smith | `_JS` |
| Ravi Kumar | `_RK` |
| Mike Chen | `_MC` |

This gives you your own isolated database (`REAL_ESTATE_ANALYTICS_JS`, etc.) so your objects don't conflict with others in a shared account.

> **Quick replace:** `Ctrl+H` → Find: `_ABC` → Replace: `_XX`

---

## Task 1: Review the Business Context (5 min)

Before building, understand what the business actually needs.

**Apex Property Group profile:**

| Field | Detail |
|-------|--------|
| Business | Commercial & residential brokerage |
| Markets | Austin, Dallas, Houston, Phoenix, Atlanta |
| Team | 8 agents across 5 offices |
| Revenue | Commission on sales (2.5%) and leases (8%) |
| Pain point | Manual Excel reporting, 2–3 day lag on performance numbers |

**Questions leadership wants to answer:**
1. Which agents are closing the most deals and generating the most commission?
2. How are our markets performing — which has the highest price-to-list ratio?
3. What property types are moving fastest (lowest days on market)?
4. Where are we over budget on operating expenses?
5. Which client segments are driving the most transaction volume?

Write down two or three additional questions *you* would ask as a property analyst. You'll test these in Task 3.

---

## Task 2: Build the Database (20 min)

### Step 1: Run the setup script

Open `setup.sql` in a Snowflake worksheet. Confirm you have replaced `_ABC` with your initials, then run the full script in sections:

**Steps 1–3** — Creates the database, schema, and all tables:
```sql
-- Confirm you're in the right context after setup
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
```
Expected output: `REAL_ESTATE_ANALYTICS_XX | ANALYTICS`

**Steps 4–7** — Loads dimension and fact data.

**Step 8** — Validation queries. Run these and confirm:

| Table | Expected Rows |
|-------|--------------|
| DIM_PROPERTY | 20 |
| DIM_AGENT | 8 |
| DIM_CLIENT | 12 |
| DIM_DATE | 366 |
| DIM_EXPENSE_CATEGORY | 10 |
| FACT_TRANSACTIONS | ~180 |
| FACT_OPERATING_EXPENSES | ~120 |

### Step 2: Explore the data

Before building the semantic layer, understand the shape of the data you're describing:

```sql
-- What does a closed transaction look like?
SELECT t.transaction_type, t.status, t.list_price, t.sale_price,
       t.days_on_market, t.commission_amount,
       a.full_name AS agent, p.market, p.property_type
FROM FACT_TRANSACTIONS t
JOIN DIM_AGENT    a ON t.agent_id    = a.agent_id
JOIN DIM_PROPERTY p ON t.property_id = p.property_id
WHERE t.status = 'Closed'
LIMIT 10;
```

```sql
-- Which markets have the most activity?
SELECT p.market, COUNT(*) AS transactions, SUM(t.commission_amount) AS total_commission
FROM FACT_TRANSACTIONS t
JOIN DIM_PROPERTY p ON t.property_id = p.property_id
WHERE t.status = 'Closed'
GROUP BY 1 ORDER BY 3 DESC;
```

---

## Task 3: Create the Semantic View (15 min)

The semantic view is what teaches Cortex Analyst how to query your data. It defines:
- **Tables** — what's available and how they relate
- **Dimensions** — attributes users can filter and group by (market, agent, property type)
- **Metrics** — pre-defined calculations (total commission, avg days on market)

### Step 1: Create the semantic view

Run **Step 9** from `setup.sql`. This creates `REAL_ESTATE_SEMANTIC_ABC` in your schema.

After running, verify it was created:
```sql
SHOW SEMANTIC VIEWS IN SCHEMA ANALYTICS;
```

### Step 2: Understand what you just defined

The semantic view maps business language onto your tables:

| Business question | Resolved via |
|------------------|-------------|
| "Which agents..." | `agent.full_name` dimension → `DIM_AGENT` |
| "...closed the most deals?" | `closed_deals` metric → `COUNT WHERE status='Closed'` |
| "In which markets?" | `agent.market` dimension → `DIM_AGENT.market` |
| "Average days on market?" | `avg_days_on_market` metric → `AVG(FACT_TRANSACTIONS.days_on_market)` |

This mapping is what lets Cortex Analyst translate natural language → SQL without ambiguity.

---

## Task 4: Set Up Cortex Analyst (15 min)

### Step 1: Open Cortex Analyst in the Snowflake UI

Navigate to: **Snowflake UI → AI & ML → Cortex Analyst**

If you don't see it, check that your role has access:
```sql
-- Grant access if needed
GRANT USAGE ON SEMANTIC VIEW ANALYTICS.REAL_ESTATE_SEMANTIC_ABC TO ROLE SYSADMIN;
```

### Step 2: Create a new Analyst

1. Click **"New Analyst"**
2. Select your semantic view: `REAL_ESTATE_ANALYTICS_XX.ANALYTICS.REAL_ESTATE_SEMANTIC_XX`
3. Name it: `Apex Property Analytics`
4. Add a description: *"Property transaction performance, agent productivity, and expense analytics for Apex Property Group"*

### Step 3: Test with Nina's questions

Start with the exact questions Nina asked:

| Question | What to look for in the result |
|----------|-------------------------------|
| "Which agents generated the most commission this year?" | Agent names ranked by commission_amount |
| "What is the average days on market by property type?" | Residential vs Commercial vs Industrial comparison |
| "Which markets have the highest price-to-list ratio?" | Percentage over or under asking price by market |
| "Where are we over budget on operating expenses?" | Negative budget variance by category |
| "How many deals closed in Q4?" | Count filtered by quarter |

For each result, click **"Show SQL"** — this is important for stakeholder trust. Nina's team will want to see that the answers are backed by real queries, not a black box.

### Step 4: Test your own questions

Try the 2–3 questions you wrote down in Task 1. Note:
- Does Cortex Analyst interpret them correctly?
- Where does it struggle? (ambiguous terms, missing synonyms in the semantic view)
- How would you improve the semantic view definition to handle them better?

---

## Task 5: Iterate on the Semantic View (optional, 5 min)

If Cortex Analyst misinterpreted any of your questions, improve the semantic view.

**Example:** If it didn't understand "top performing agents":
- Add a synonym to the `agent_name` dimension: `WITH SYNONYMS ('agent', 'broker', 'realtor')`
- Add a `COMMENT` to the `closed_deals` metric: *"Number of deals successfully closed — use for agent performance"*

Then re-run the semantic view DDL and test again. This iterative refinement is a normal part of building a production Cortex Analyst deployment.

---

## Validation Checklist

Before moving on:

- [ ] Database `REAL_ESTATE_ANALYTICS_XX` created with correct `_XX` suffix
- [ ] All 7 tables created and loaded (row counts match expected)
- [ ] Semantic view created — `SHOW SEMANTIC VIEWS` confirms it exists
- [ ] Cortex Analyst answers Nina's questions with correct SQL and results
- [ ] "Show SQL" confirms the generated queries are logically correct
- [ ] Tested at least one question of your own

---

## Key Takeaways

1. **The semantic view does the heavy lifting** — the quality of Cortex Analyst responses is directly tied to how well you describe your data model with dimensions, metrics, and comments.
2. **Show the SQL** — non-technical stakeholders trust AI more when they can see the query it generated. Always surface this in demos.
3. **Natural language is imprecise** — you'll need to iterate on synonyms and descriptions. Build this iteration into your timeline.
4. **Dimensions vs metrics** — dimensions are things to filter/group by; metrics are things to measure. Getting this boundary right avoids ambiguous queries.

---

## Common Issues

| Issue | Fix |
|-------|-----|
| `CREATE SEMANTIC VIEW` fails | Verify the syntax against current Snowflake docs — the feature was in preview and syntax may have minor version differences |
| "Object not found" in Cortex Analyst | Confirm `USE DATABASE / SCHEMA` context and that the semantic view was created under `_ABC` suffix |
| Wrong metric returned | Add a `COMMENT` to the metric clarifying when to use it — Cortex Analyst uses comments to choose between similar metrics |
| Date filtering not working | Ensure `DIM_DATE.full_date` is exposed as a dimension and the `MANY → ONE` relationship to fact tables is defined |

---

## What's Next

Once the basic Cortex Analyst is working, the natural next steps are:
- **Refine the semantic view** — add synonyms, better comments, more granular dimensions
- **Snowflake Intelligence** — wrap the Cortex Analyst in a conversational chat UI for leadership
- **Production data** — replace the generated sample data with a real ingestion pipeline using `framework.py`
- **dbt layer** — model the raw transaction data through bronze → silver → gold before exposing to the semantic layer
