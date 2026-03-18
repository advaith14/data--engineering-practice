-- ================================================================
-- Snowflake Cortex Analyst — Real Estate Portfolio Analytics
-- Setup Script
-- ================================================================
--
-- BEFORE RUNNING
-- ==============
-- Find and replace  _ABC  with your own initials throughout this
-- entire file before executing.
--
-- Example:
--   Your name: Jane Smith  → use _JS
--   Your name: Ravi Kumar  → use _RK
--
-- This gives every participant their own isolated database so
-- objects don't collide in a shared Snowflake account.
--
-- Quick replace:  Ctrl+H  →  Find: _ABC  →  Replace: _XX
-- ================================================================


-- ================================================================
-- STEP 1 — DATABASE & SCHEMA SETUP
-- ================================================================

CREATE DATABASE IF NOT EXISTS REAL_ESTATE_ANALYTICS_ABC
  COMMENT = 'Apex Property Group — Cortex Analyst POC';

USE DATABASE REAL_ESTATE_ANALYTICS_ABC;

CREATE SCHEMA IF NOT EXISTS ANALYTICS
  COMMENT = 'Property portfolio analytics — dimensions, facts, semantic layer';

USE SCHEMA ANALYTICS;


-- ================================================================
-- STEP 2 — DIMENSION TABLES
-- ================================================================

CREATE OR REPLACE TABLE DIM_PROPERTY (
    property_id       NUMBER        NOT NULL PRIMARY KEY,
    address           VARCHAR(200),
    city              VARCHAR(100),
    state             VARCHAR(50),
    market            VARCHAR(100)  COMMENT 'Metro market area',
    property_type     VARCHAR(50)   COMMENT 'Residential | Commercial | Industrial',
    sub_type          VARCHAR(50)   COMMENT 'Single Family | Condo | Office | Retail | Warehouse',
    size_sqft         NUMBER,
    year_built        NUMBER,
    bedrooms          NUMBER,
    bathrooms         NUMBER,
    list_price        NUMBER(12,2)  COMMENT 'Current or last listed asking price'
);

CREATE OR REPLACE TABLE DIM_AGENT (
    agent_id          NUMBER        NOT NULL PRIMARY KEY,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    full_name         VARCHAR(200),
    office_location   VARCHAR(100),
    market            VARCHAR(100),
    years_experience  NUMBER,
    specialization    VARCHAR(100)  COMMENT 'Residential | Commercial | Industrial | Mixed'
);

CREATE OR REPLACE TABLE DIM_CLIENT (
    client_id         NUMBER        NOT NULL PRIMARY KEY,
    client_name       VARCHAR(200),
    client_type       VARCHAR(50)   COMMENT 'Buyer | Seller | Investor | Landlord | Tenant',
    segment           VARCHAR(50)   COMMENT 'Individual | Corporate | Institutional',
    market            VARCHAR(100)
);

CREATE OR REPLACE TABLE DIM_DATE (
    date_id           NUMBER        NOT NULL PRIMARY KEY,
    full_date         DATE,
    year              NUMBER,
    quarter_num       NUMBER,
    quarter_label     VARCHAR(10),  -- Q1 2024
    month_num         NUMBER,
    month_name        VARCHAR(20),
    day_of_week       NUMBER,       -- 0=Sun ... 6=Sat
    day_name          VARCHAR(20),
    is_weekday        BOOLEAN
);

CREATE OR REPLACE TABLE DIM_EXPENSE_CATEGORY (
    category_id       NUMBER        NOT NULL PRIMARY KEY,
    category_name     VARCHAR(100),
    category_group    VARCHAR(100)  COMMENT 'Operations | Marketing | Technology | Compliance'
);


-- ================================================================
-- STEP 3 — FACT TABLES
-- ================================================================

CREATE OR REPLACE TABLE FACT_TRANSACTIONS (
    transaction_id    NUMBER        NOT NULL PRIMARY KEY,
    property_id       NUMBER        REFERENCES DIM_PROPERTY(property_id),
    agent_id          NUMBER        REFERENCES DIM_AGENT(agent_id),
    client_id         NUMBER        REFERENCES DIM_CLIENT(client_id),
    date_id           NUMBER        REFERENCES DIM_DATE(date_id),
    transaction_type  VARCHAR(20)   COMMENT 'Sale | Lease',
    list_price        NUMBER(12,2),
    sale_price        NUMBER(12,2),
    days_on_market    NUMBER,
    commission_rate   NUMBER(5,4)   COMMENT 'E.g. 0.0250 = 2.5%',
    commission_amount NUMBER(10,2),
    status            VARCHAR(20)   COMMENT 'Closed | Pending | Withdrawn'
);

CREATE OR REPLACE TABLE FACT_OPERATING_EXPENSES (
    expense_id        NUMBER        NOT NULL PRIMARY KEY,
    category_id       NUMBER        REFERENCES DIM_EXPENSE_CATEGORY(category_id),
    date_id           NUMBER        REFERENCES DIM_DATE(date_id),
    market            VARCHAR(100),
    actual_amount     NUMBER(10,2),
    budget_amount     NUMBER(10,2),
    budget_variance   NUMBER(10,2)  COMMENT 'Positive = under budget'
);


-- ================================================================
-- STEP 4 — LOAD DIMENSION DATA
-- ================================================================

INSERT INTO DIM_PROPERTY VALUES
  (1,  '142 Oak Street',         'Austin',       'TX', 'Austin Metro',       'Residential',  'Single Family', 2450, 2005, 4, 3, 785000),
  (2,  '88 Riverside Drive',     'Austin',       'TX', 'Austin Metro',       'Residential',  'Single Family', 3100, 2012, 5, 4, 1150000),
  (3,  '301 Congress Ave',       'Austin',       'TX', 'Austin Metro',       'Commercial',   'Office',        18500, 1998, NULL, NULL, 6200000),
  (4,  '77 South Lamar Blvd',    'Austin',       'TX', 'Austin Metro',       'Commercial',   'Retail',        4200, 2001, NULL, NULL, 1850000),
  (5,  '5510 N Interstate 35',   'Austin',       'TX', 'Austin Metro',       'Industrial',   'Warehouse',     42000, 2008, NULL, NULL, 4800000),
  (6,  '210 Main Street',        'Dallas',       'TX', 'DFW Metroplex',      'Residential',  'Condo',         1650, 2018, 2, 2, 420000),
  (7,  '1405 Commerce Street',   'Dallas',       'TX', 'DFW Metroplex',      'Residential',  'Single Family', 2900, 2003, 4, 3, 695000),
  (8,  '9000 Legacy Drive',      'Dallas',       'TX', 'DFW Metroplex',      'Commercial',   'Office',        32000, 2015, NULL, NULL, 11500000),
  (9,  '4800 Airport Freeway',   'Dallas',       'TX', 'DFW Metroplex',      'Industrial',   'Warehouse',     78000, 2010, NULL, NULL, 9200000),
  (10, '15 Harbor View Court',   'Houston',      'TX', 'Houston Metro',      'Residential',  'Single Family', 3300, 2007, 5, 4, 920000),
  (11, '620 Travis Street',      'Houston',      'TX', 'Houston Metro',      'Commercial',   'Office',        24000, 2000, NULL, NULL, 7800000),
  (12, '8800 W Sam Houston Pkwy','Houston',      'TX', 'Houston Metro',      'Industrial',   'Warehouse',     96000, 2012, NULL, NULL, 13500000),
  (13, '700 Brazos Street',      'Houston',      'TX', 'Houston Metro',      'Commercial',   'Retail',        5800, 2004, NULL, NULL, 2400000),
  (14, '22 Sunset Ridge Lane',   'Phoenix',      'AZ', 'Phoenix Metro',      'Residential',  'Single Family', 2700, 2010, 4, 3, 610000),
  (15, '1200 N Tatum Blvd',      'Phoenix',      'AZ', 'Phoenix Metro',      'Commercial',   'Retail',        6500, 2006, NULL, NULL, 2100000),
  (16, '480 S Power Road',       'Phoenix',      'AZ', 'Phoenix Metro',      'Industrial',   'Warehouse',     55000, 2015, NULL, NULL, 6800000),
  (17, '33 Peachtree Street',    'Atlanta',      'GA', 'Atlanta Metro',      'Commercial',   'Office',        28500, 2016, NULL, NULL, 9900000),
  (18, '120 Ponce De Leon Ave',  'Atlanta',      'GA', 'Atlanta Metro',      'Residential',  'Condo',         1820, 2020, 2, 2, 480000),
  (19, '5000 Fulton Industrial', 'Atlanta',      'GA', 'Atlanta Metro',      'Industrial',   'Warehouse',     68000, 2011, NULL, NULL, 8100000),
  (20, '640 N LaSalle Drive',    'Chicago',      'IL', 'Chicago Metro',      'Commercial',   'Office',        41000, 1995, NULL, NULL, 14200000);

INSERT INTO DIM_AGENT VALUES
  (1,  'Marcus',   'Reyes',     'Marcus Reyes',     'Austin HQ',    'Austin Metro',  12, 'Residential'),
  (2,  'Jennifer', 'Walsh',     'Jennifer Walsh',   'Austin HQ',    'Austin Metro',   8, 'Commercial'),
  (3,  'Daniel',   'Okonkwo',   'Daniel Okonkwo',   'Dallas Office','DFW Metroplex', 15, 'Commercial'),
  (4,  'Priya',    'Sharma',    'Priya Sharma',     'Dallas Office','DFW Metroplex',  6, 'Industrial'),
  (5,  'Carlos',   'Mendes',    'Carlos Mendes',    'Houston Office','Houston Metro', 10, 'Mixed'),
  (6,  'Rachel',   'Kim',       'Rachel Kim',       'Houston Office','Houston Metro',  4, 'Residential'),
  (7,  'Thomas',   'Blackwell', 'Thomas Blackwell', 'Phoenix Office','Phoenix Metro',  9, 'Commercial'),
  (8,  'Alicia',   'Vance',     'Alicia Vance',     'Atlanta Office','Atlanta Metro',  7, 'Mixed');

INSERT INTO DIM_CLIENT VALUES
  (1,  'Hartwell Capital Partners',  'Investor',   'Institutional', 'Austin Metro'),
  (2,  'Sarah & David Nguyen',       'Buyer',      'Individual',    'Austin Metro'),
  (3,  'TechNorth Properties LLC',   'Investor',   'Corporate',     'DFW Metroplex'),
  (4,  'Greenfield Development Co.', 'Buyer',      'Corporate',     'DFW Metroplex'),
  (5,  'Mark & Claire Osei',         'Seller',     'Individual',    'Houston Metro'),
  (6,  'Starlight Retail Group',     'Tenant',     'Corporate',     'Houston Metro'),
  (7,  'Vector Industrial Fund',     'Investor',   'Institutional', 'Houston Metro'),
  (8,  'Desert Sun Holdings',        'Buyer',      'Corporate',     'Phoenix Metro'),
  (9,  'Apex Residential Trust',     'Investor',   'Institutional', 'Atlanta Metro'),
  (10, 'Emma Kowalski',              'Buyer',      'Individual',    'Atlanta Metro'),
  (11, 'Midwest Industrial REIT',    'Investor',   'Institutional', 'Chicago Metro'),
  (12, 'Rivera & Sons Construction', 'Seller',     'Corporate',     'Austin Metro');

INSERT INTO DIM_EXPENSE_CATEGORY VALUES
  (1, 'Agent Commissions',       'Operations'),
  (2, 'Office Rent & Utilities', 'Operations'),
  (3, 'Staff Salaries',          'Operations'),
  (4, 'Marketing & Advertising', 'Marketing'),
  (5, 'CRM & PropTech Tools',    'Technology'),
  (6, 'MLS Fees & Listings',     'Operations'),
  (7, 'Legal & Compliance',      'Compliance'),
  (8, 'Travel & Entertainment',  'Operations'),
  (9, 'Data & Analytics',        'Technology'),
  (10,'Training & Certification','Operations');


-- ================================================================
-- STEP 5 — LOAD DATE DIMENSION (full year 2024)
-- ================================================================

INSERT INTO DIM_DATE
WITH date_series AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4())           AS date_id,
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2024-01-01') AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 366))  -- 2024 is a leap year
)
SELECT
    date_id,
    full_date,
    YEAR(full_date)                                                              AS year,
    QUARTER(full_date)                                                           AS quarter_num,
    'Q' || QUARTER(full_date)::VARCHAR || ' ' || YEAR(full_date)::VARCHAR        AS quarter_label,
    MONTH(full_date)                                                             AS month_num,
    MONTHNAME(full_date)                                                         AS month_name,
    DAYOFWEEK(full_date)                                                         AS day_of_week,
    DAYNAME(full_date)                                                           AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN FALSE ELSE TRUE END            AS is_weekday
FROM date_series
WHERE full_date <= '2024-12-31';


-- ================================================================
-- STEP 6 — GENERATE FACT: TRANSACTIONS (~180 rows)
-- ================================================================

INSERT INTO FACT_TRANSACTIONS
WITH base AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4())                              AS transaction_id,
        (ABS(MOD(RANDOM(), 20)) + 1)::NUMBER                            AS property_id,
        (ABS(MOD(RANDOM(),  8)) + 1)::NUMBER                            AS agent_id,
        (ABS(MOD(RANDOM(), 12)) + 1)::NUMBER                            AS client_id,
        (ABS(MOD(RANDOM(), 366)) + 1)::NUMBER                           AS date_id,
        CASE WHEN MOD(ABS(RANDOM()), 5) = 0 THEN 'Lease' ELSE 'Sale' END AS transaction_type,
        UNIFORM(180000, 14500000, RANDOM())::NUMBER(12,2)               AS list_price_raw,
        UNIFORM(1, 120, RANDOM())::NUMBER                               AS days_on_market,
        CASE WHEN MOD(ABS(RANDOM()), 10) = 0 THEN 'Withdrawn'
             WHEN MOD(ABS(RANDOM()), 10) = 1 THEN 'Pending'
             ELSE 'Closed' END                                           AS status
    FROM TABLE(GENERATOR(ROWCOUNT => 180))
),
with_price AS (
    SELECT *,
        -- Sale price within ±8% of list price
        ROUND(list_price_raw * UNIFORM(0.92, 1.08, RANDOM()), -3)::NUMBER(12,2) AS sale_price,
        CASE
            WHEN transaction_type = 'Sale'  THEN 0.025   -- 2.5% commission for sales
            WHEN transaction_type = 'Lease' THEN 0.08    -- one month's rent (8% proxy)
        END AS commission_rate
    FROM base
)
SELECT
    transaction_id,
    property_id,
    agent_id,
    client_id,
    date_id,
    transaction_type,
    list_price_raw                                       AS list_price,
    CASE WHEN status = 'Closed' THEN sale_price ELSE NULL END AS sale_price,
    days_on_market,
    commission_rate,
    CASE WHEN status = 'Closed'
         THEN ROUND(sale_price * commission_rate, 2)
         ELSE NULL
    END                                                  AS commission_amount,
    status
FROM with_price;


-- ================================================================
-- STEP 7 — GENERATE FACT: OPERATING EXPENSES (~120 rows)
-- ================================================================

INSERT INTO FACT_OPERATING_EXPENSES
WITH base AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4())                              AS expense_id,
        (ABS(MOD(RANDOM(), 10)) + 1)::NUMBER                            AS category_id,
        (ABS(MOD(RANDOM(), 366)) + 1)::NUMBER                           AS date_id,
        CASE ABS(MOD(RANDOM(), 5))
            WHEN 0 THEN 'Austin Metro'
            WHEN 1 THEN 'DFW Metroplex'
            WHEN 2 THEN 'Houston Metro'
            WHEN 3 THEN 'Phoenix Metro'
            ELSE        'Atlanta Metro'
        END                                                              AS market,
        UNIFORM(5000, 120000, RANDOM())::NUMBER(10,2)                   AS budget_amount
    FROM TABLE(GENERATOR(ROWCOUNT => 120))
)
SELECT
    expense_id,
    category_id,
    date_id,
    market,
    -- Actual spend: 80%–120% of budget to create realistic variances
    ROUND(budget_amount * UNIFORM(0.80, 1.20, RANDOM()), 2)::NUMBER(10,2) AS actual_amount,
    budget_amount,
    ROUND(budget_amount - (budget_amount * UNIFORM(0.80, 1.20, RANDOM())), 2)::NUMBER(10,2) AS budget_variance
FROM base;


-- ================================================================
-- STEP 8 — VALIDATION QUERIES
-- ================================================================

-- Row counts
SELECT 'DIM_PROPERTY'          AS table_name, COUNT(*) AS row_count FROM DIM_PROPERTY
UNION ALL SELECT 'DIM_AGENT',           COUNT(*) FROM DIM_AGENT
UNION ALL SELECT 'DIM_CLIENT',          COUNT(*) FROM DIM_CLIENT
UNION ALL SELECT 'DIM_DATE',            COUNT(*) FROM DIM_DATE
UNION ALL SELECT 'DIM_EXPENSE_CATEGORY',COUNT(*) FROM DIM_EXPENSE_CATEGORY
UNION ALL SELECT 'FACT_TRANSACTIONS',   COUNT(*) FROM FACT_TRANSACTIONS
UNION ALL SELECT 'FACT_OPERATING_EXPENSES', COUNT(*) FROM FACT_OPERATING_EXPENSES
ORDER BY 1;

-- Transaction summary
SELECT
    transaction_type,
    status,
    COUNT(*)                         AS transaction_count,
    SUM(commission_amount)           AS total_commission,
    ROUND(AVG(days_on_market), 1)    AS avg_dom,
    ROUND(AVG(sale_price / NULLIF(list_price,0) * 100), 2) AS avg_price_to_list_pct
FROM FACT_TRANSACTIONS
GROUP BY 1, 2
ORDER BY 1, 2;

-- Commission revenue by market (via agent)
SELECT
    a.market,
    COUNT(t.transaction_id)          AS closed_deals,
    SUM(t.commission_amount)         AS total_commission
FROM FACT_TRANSACTIONS t
JOIN DIM_AGENT a ON t.agent_id = a.agent_id
WHERE t.status = 'Closed'
GROUP BY 1
ORDER BY 3 DESC;

-- Budget variance by category
SELECT
    ec.category_name,
    ec.category_group,
    ROUND(SUM(e.actual_amount), 0)   AS total_actual,
    ROUND(SUM(e.budget_amount), 0)   AS total_budget,
    ROUND(SUM(e.budget_variance), 0) AS total_variance,
    CASE WHEN SUM(e.budget_variance) < 0 THEN 'OVER BUDGET' ELSE 'UNDER BUDGET' END AS status
FROM FACT_OPERATING_EXPENSES e
JOIN DIM_EXPENSE_CATEGORY ec ON e.category_id = ec.category_id
GROUP BY 1, 2
ORDER BY total_variance ASC;


-- ================================================================
-- STEP 9 — CREATE SEMANTIC VIEW FOR CORTEX ANALYST
-- ================================================================
-- NOTE: CREATE SEMANTIC VIEW is a Snowflake preview/GA feature.
-- If you receive a syntax error, verify the current syntax in
-- Snowflake Docs: docs.snowflake.com → Search "CREATE SEMANTIC VIEW"
-- The syntax below is based on the GA release (2024).
-- ================================================================

CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.REAL_ESTATE_SEMANTIC_ABC
  COMMENT = 'Apex Property Group — semantic layer for Cortex Analyst'

  TABLES (
    DIM_PROPERTY       AS property       PRIMARY KEY (property_id),
    DIM_AGENT          AS agent          PRIMARY KEY (agent_id),
    DIM_CLIENT         AS client         PRIMARY KEY (client_id),
    DIM_DATE           AS date           PRIMARY KEY (date_id),
    FACT_TRANSACTIONS  AS transaction    PRIMARY KEY (transaction_id),
    FACT_OPERATING_EXPENSES AS expense   PRIMARY KEY (expense_id)
  )

  RELATIONSHIPS (
    MANY transaction(property_id) TO ONE property(property_id),
    MANY transaction(agent_id)    TO ONE agent(agent_id),
    MANY transaction(client_id)   TO ONE client(client_id),
    MANY transaction(date_id)     TO ONE date(date_id),
    MANY expense(date_id)         TO ONE date(date_id)
  )

  DIMENSIONS (
    property.market          AS property_market        COMMENT 'Metro market area of the property',
    property.property_type   AS property_type          COMMENT 'Residential, Commercial, or Industrial',
    property.sub_type        AS property_sub_type      COMMENT 'E.g. Single Family, Office, Warehouse',
    property.city            AS property_city,
    agent.full_name          AS agent_name             COMMENT 'Agent full name',
    agent.office_location    AS agent_office,
    agent.market             AS agent_market           COMMENT 'Primary market the agent serves',
    agent.specialization     AS agent_specialization,
    client.client_name       AS client_name,
    client.client_type       AS client_type            COMMENT 'Buyer, Seller, Investor, Landlord, Tenant',
    client.segment           AS client_segment         COMMENT 'Individual, Corporate, or Institutional',
    date.full_date           AS transaction_date,
    date.year                AS transaction_year,
    date.quarter_label       AS transaction_quarter,
    date.month_name          AS transaction_month,
    transaction.transaction_type AS transaction_type   COMMENT 'Sale or Lease',
    transaction.status       AS transaction_status
  )

  METRICS (
    SUM(transaction.commission_amount)                     AS total_commission_revenue
      COMMENT 'Total commission earned on closed transactions',
    COUNT(transaction.transaction_id)                      AS total_transactions
      COMMENT 'Total number of transactions',
    SUM(CASE WHEN transaction.status = ''Closed'' THEN 1 ELSE 0 END)
                                                           AS closed_deals
      COMMENT 'Count of successfully closed deals',
    AVG(transaction.days_on_market)                        AS avg_days_on_market
      COMMENT 'Average days from listing to close',
    AVG(transaction.sale_price / NULLIF(transaction.list_price, 0) * 100)
                                                           AS avg_price_to_list_ratio
      COMMENT 'Average sale price as a percentage of list price — above 100% = over asking',
    SUM(expense.actual_amount)                             AS total_operating_expenses
      COMMENT 'Total actual operating expenses incurred',
    SUM(expense.budget_variance)                           AS total_budget_variance
      COMMENT 'Positive = under budget; Negative = over budget'
  );


-- ================================================================
-- STEP 10 — GRANT ACCESS (for shared Snowflake accounts)
-- ================================================================

GRANT USAGE ON DATABASE  REAL_ESTATE_ANALYTICS_ABC   TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA    ANALYTICS                   TO ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS       TO ROLE SYSADMIN;
GRANT SELECT ON SEMANTIC VIEW ANALYTICS.REAL_ESTATE_SEMANTIC_ABC TO ROLE SYSADMIN;


-- ================================================================
-- STEP 11 — CORTEX ANALYST / SNOWFLAKE INTELLIGENCE SETUP
-- ================================================================
-- Cortex Agent creation and Snowflake Intelligence configuration
-- is done through the Snowflake UI.
--
-- TO SET UP CORTEX ANALYST:
--  1. In Snowflake UI → Cortex Analyst (left nav)
--  2. Create new Analyst → Select semantic view:
--     REAL_ESTATE_ANALYTICS_ABC.ANALYTICS.REAL_ESTATE_SEMANTIC_ABC
--  3. Name your agent: "Apex Property Analytics"
--
-- SAMPLE QUESTIONS TO TEST:
--  · "Which agents generated the most commission revenue this year?"
--  · "What is the average days on market by property type?"
--  · "Which markets have the highest price-to-list ratio?"
--  · "Show me total commission revenue by quarter"
--  · "Which clients closed the most transactions?"
--  · "What expense categories are over budget?"
--  · "Compare residential vs commercial transaction volumes"
-- ================================================================
