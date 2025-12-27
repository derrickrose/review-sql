# SQL Interview Mastery - PART 2: Expert Level
## From Netflix-Ready to FAANG Staff Engineer Level

> **Part 1 got you to 95% coverage. Part 2 takes you to 200% - the level where YOU interview THEM.**

---

## 🎯 What Part 2 Covers

**Part 1:** You can pass Netflix SQL rounds  
**Part 2:** You can design their data infrastructure

**New Skills You'll Master:**
- 🔥 Query optimization & performance tuning
- 🔥 Advanced window functions (frames, QUALIFY)
- 🔥 Complex date/time analytics
- 🔥 Data quality & anomaly detection
- 🔥 Self-joins and advanced patterns
- 🔥 Analytical functions interviewers rarely see
- 🔥 Real production debugging scenarios
- 🔥 Staff+ level system design with SQL

**Who needs Part 2:**
- ✅ Senior Data Engineer roles (Netflix, Meta, Google)
- ✅ Staff+ Analytics Engineer positions
- ✅ Anyone targeting $200k+ TC roles
- ✅ Those who crushed Part 1 and want mastery

---

## 📋 Table of Contents - Part 2

1. [Advanced Window Functions (23-28)](#-advanced-window-functions-queries-23-28)
2. [Self-Joins & Complex Patterns (29-33)](#-self-joins--complex-patterns-queries-29-33)
3. [Date/Time Analytics (34-38)](#-datetime-analytics-queries-34-38)
4. [Data Quality & Validation (39-42)](#-data-quality--validation-queries-39-42)
5. [Query Optimization (43-46)](#-query-optimization-queries-43-46)
6. [Staff Engineer Questions (47-50)](#-staff-engineer-level-queries-47-50)
7. [Production Debugging Scenarios](#-production-debugging-scenarios)
8. [Performance Tuning Guide](#-performance-tuning-guide)

---

## 🚀 ADVANCED WINDOW FUNCTIONS (Queries 23-28)

### Query 23: LAG/LEAD - Access Previous/Next Rows
**Interview Context:** "Compare with previous period"  
**Netflix Example:** "Month-over-month revenue growth"

```sql
-- Show each subscription with previous month's revenue for comparison
SELECT
    DATE_TRUNC('month', subscription_date) AS month,
    SUM(total_amount) AS monthly_revenue,
    LAG(SUM(total_amount)) OVER (ORDER BY DATE_TRUNC('month', subscription_date)) AS prev_month_revenue,
    SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY DATE_TRUNC('month', subscription_date)) AS revenue_change,
    ROUND(
        100.0 * (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY DATE_TRUNC('month', subscription_date))) 
        / LAG(SUM(total_amount)) OVER (ORDER BY DATE_TRUNC('month', subscription_date)),
        2
    ) AS growth_rate_pct
FROM subscriptions
GROUP BY DATE_TRUNC('month', subscription_date)
ORDER BY month;
```

**LAG vs LEAD:**
- `LAG(column, offset)` - Look backward (previous row)
- `LEAD(column, offset)` - Look forward (next row)
- `offset` defaults to 1 (can be 2, 3, etc.)

**Real Netflix Interview:** "Calculate user watch streak (consecutive days)"
```sql
SELECT
    user_id,
    watch_date,
    watch_date - LAG(watch_date) OVER (PARTITION BY user_id ORDER BY watch_date) AS days_since_last_watch,
    CASE 
        WHEN watch_date - LAG(watch_date) OVER (PARTITION BY user_id ORDER BY watch_date) = 1 
        THEN 'Streak Continues'
        ELSE 'Streak Broken'
    END AS streak_status
FROM viewing_history;
```

---

### Query 24: FIRST_VALUE/LAST_VALUE - Frame Boundaries
**Interview Context:** "Get first/last value in a window"  
**Netflix Example:** "Compare each creator's salary to dept min/max"

```sql
SELECT
    first_name,
    last_name,
    department_id,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department_id 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS dept_max_salary,
    LAST_VALUE(salary) OVER (
        PARTITION BY department_id 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS dept_min_salary,
    salary - LAST_VALUE(salary) OVER (
        PARTITION BY department_id 
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS salary_above_min
FROM content_creators
ORDER BY department_id, salary DESC;
```

**Critical Frame Specification:**
```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- Without this, LAST_VALUE only sees current row!
```

**Common mistake:** Forgetting the frame → wrong results

---

### Query 25: NTILE - Divide into N Equal Groups
**Interview Context:** "Create quartiles, percentiles"  
**Netflix Example:** "Divide users into 4 engagement tiers"

```sql
WITH user_engagement AS (
    SELECT
        u.user_id,
        u.user_name,
        COUNT(v.view_id) AS total_views,
        SUM(v.minutes_watched) AS total_minutes
    FROM streaming_users u
    LEFT JOIN viewing_history v ON u.user_id = v.user_id
    GROUP BY u.user_id, u.user_name
)
SELECT
    user_name,
    total_views,
    total_minutes,
    NTILE(4) OVER (ORDER BY total_minutes DESC) AS engagement_quartile,
    CASE NTILE(4) OVER (ORDER BY total_minutes DESC)
        WHEN 1 THEN 'Top 25% - Super Users'
        WHEN 2 THEN 'Top 50% - Active Users'
        WHEN 3 THEN 'Top 75% - Regular Users'
        WHEN 4 THEN 'Bottom 25% - At Risk'
    END AS user_tier
FROM user_engagement
ORDER BY total_minutes DESC;
```

**Use cases:**
- A/B test groups (divide users into equal test groups)
- Percentile analysis (NTILE(100) for percentiles)
- Risk segmentation

**Netflix Interview:** "Create 10 equal-sized cohorts for recommendation testing"

---

### Query 26: Moving Averages - Window Frames
**Interview Context:** "Smooth out trends, detect anomalies"  
**Netflix Example:** "7-day moving average of daily signups"

```sql
WITH daily_signups AS (
    SELECT
        signup_date,
        COUNT(*) AS new_users
    FROM streaming_users
    GROUP BY signup_date
)
SELECT
    signup_date,
    new_users,
    AVG(new_users) OVER (
        ORDER BY signup_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day,
    AVG(new_users) OVER (
        ORDER BY signup_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30day
FROM daily_signups
ORDER BY signup_date;
```

**Window Frame Types:**
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` - Last 7 rows
- `ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING` - Centered window
- `RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW` - Date-based

**Real Interview:** "Detect unusual spikes using 7-day MA"
```sql
-- Flag days where signups > 150% of 7-day average
SELECT *,
    CASE 
        WHEN new_users > 1.5 * moving_avg_7day THEN 'SPIKE DETECTED'
        ELSE 'Normal'
    END AS anomaly_flag
FROM (moving average query above)
WHERE new_users > 1.5 * moving_avg_7day;
```

---

### Query 27: PERCENT_RANK & CUME_DIST
**Interview Context:** "Relative ranking and percentiles"  
**Netflix Example:** "What percentile is each creator's salary?"

```sql
SELECT
    first_name,
    last_name,
    salary,
    PERCENT_RANK() OVER (ORDER BY salary) AS percent_rank,
    CUME_DIST() OVER (ORDER BY salary) AS cumulative_distribution,
    ROUND(100 * PERCENT_RANK() OVER (ORDER BY salary), 1) AS percentile
FROM content_creators
ORDER BY salary DESC;
```

**Difference:**
- `PERCENT_RANK()`: 0 to 1 (relative rank: (rank - 1) / (total - 1))
- `CUME_DIST()`: 0 to 1 (cumulative: how many ≤ this value / total)

**Netflix Use Case:** "Show users their watch time percentile"
```sql
WITH user_watch_time AS (
    SELECT 
        user_id,
        SUM(minutes_watched) AS total_minutes
    FROM viewing_history
    GROUP BY user_id
)
SELECT
    user_id,
    total_minutes,
    CONCAT(
        'You watched more than ',
        ROUND(100 * PERCENT_RANK() OVER (ORDER BY total_minutes), 0),
        '% of users'
    ) AS percentile_message
FROM user_watch_time;
```

---

### Query 28: QUALIFY Clause (Advanced Filtering)
**Interview Context:** "Filter window function results directly"  
**Netflix Example:** "Get only the top earner per department"

```sql
-- Standard way (subquery)
SELECT * FROM (
    SELECT 
        first_name,
        department_id,
        salary,
        RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rk
    FROM content_creators
) WHERE rk = 1;

-- QUALIFY way (cleaner, supported in Snowflake, BigQuery)
SELECT 
    first_name,
    department_id,
    salary
FROM content_creators
QUALIFY RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) = 1;
```

**Note:** Not all databases support QUALIFY (Snowflake, BigQuery yes; PostgreSQL no)

**Interview advantage:** Shows you know modern SQL features

---

## 🔄 SELF-JOINS & COMPLEX PATTERNS (Queries 29-33)

### Query 29: Self-Join - Find Pairs
**Interview Context:** "Compare rows within same table"  
**Netflix Example:** "Find creators in same department with similar salaries"

```sql
SELECT
    c1.first_name AS person1,
    c2.first_name AS person2,
    c1.department_id,
    c1.salary AS salary1,
    c2.salary AS salary2,
    ABS(c1.salary - c2.salary) AS salary_difference
FROM content_creators c1
JOIN content_creators c2 
    ON c1.department_id = c2.department_id
    AND c1.creator_id < c2.creator_id  -- Avoid duplicates and self-pairing
    AND ABS(c1.salary - c2.salary) < 5000  -- Within $5k of each other
ORDER BY c1.department_id, salary_difference;
```

**Critical:** `c1.creator_id < c2.creator_id` prevents:
- Self-pairing (Alice with Alice)
- Duplicates (Alice-Bob and Bob-Alice)

**Real Netflix Interview:** "Find users who watched the same content on the same day"
```sql
SELECT
    v1.user_id AS user1,
    v2.user_id AS user2,
    v1.content_id,
    v1.watch_date
FROM viewing_history v1
JOIN viewing_history v2
    ON v1.content_id = v2.content_id
    AND v1.watch_date = v2.watch_date
    AND v1.user_id < v2.user_id
LIMIT 10;
```

---

### Query 30: Gap and Island Problem
**Interview Context:** "Find consecutive sequences and gaps"  
**Netflix Example:** "Find continuous subscription periods (islands) and cancellation gaps"

```sql
WITH subscription_sequences AS (
    SELECT
        user_id,
        subscription_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY subscription_date) AS rn,
        subscription_date - INTERVAL '1 month' * ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY subscription_date) AS island_id
    FROM subscriptions
    WHERE status = 'Active'
)
SELECT
    user_id,
    MIN(subscription_date) AS streak_start,
    MAX(subscription_date) AS streak_end,
    COUNT(*) AS consecutive_months
FROM subscription_sequences
GROUP BY user_id, island_id
HAVING COUNT(*) >= 3  -- Only streaks of 3+ months
ORDER BY user_id, streak_start;
```

**What's happening:**
1. `ROW_NUMBER()` assigns sequential numbers
2. Subtract it from date to create "island_id"
3. Same island_id = consecutive sequence
4. GROUP BY island_id to find ranges

**THIS PATTERN IS ASKED AT GOOGLE/META/NETFLIX FOR SENIOR ROLES**

---

### Query 31: Running Total with Reset
**Interview Context:** "Cumulative sum that resets per group"  
**Netflix Example:** "Monthly cumulative revenue per department"

```sql
SELECT
    d.department_name,
    DATE_TRUNC('month', s.subscription_date) AS month,
    SUM(s.total_amount) AS monthly_revenue,
    SUM(SUM(s.total_amount)) OVER (
        PARTITION BY d.department_id, EXTRACT(YEAR FROM s.subscription_date)
        ORDER BY DATE_TRUNC('month', s.subscription_date)
    ) AS ytd_revenue
FROM subscriptions s
JOIN streaming_users u ON s.user_id = u.user_id
JOIN viewing_history v ON u.user_id = v.user_id
JOIN content_catalog c ON v.content_id = c.content_id
JOIN departments d ON c.department_id = d.department_id
GROUP BY d.department_id, d.department_name, DATE_TRUNC('month', s.subscription_date)
ORDER BY d.department_name, month;
```

**Key:** `PARTITION BY department_id, year` resets the running total each year per department

---

### Query 32: Conditional Aggregation - Pivot Tables
**Interview Context:** "Transpose rows to columns"  
**Netflix Example:** "Show subscription counts by plan type as columns"

```sql
SELECT
    DATE_TRUNC('month', subscription_date) AS month,
    COUNT(*) AS total_subscriptions,
    SUM(CASE WHEN plan_type = 'Basic' THEN 1 ELSE 0 END) AS basic_count,
    SUM(CASE WHEN plan_type = 'Standard' THEN 1 ELSE 0 END) AS standard_count,
    SUM(CASE WHEN plan_type = 'Premium' THEN 1 ELSE 0 END) AS premium_count,
    SUM(CASE WHEN plan_type = 'Basic' THEN total_amount ELSE 0 END) AS basic_revenue,
    SUM(CASE WHEN plan_type = 'Standard' THEN total_amount ELSE 0 END) AS standard_revenue,
    SUM(CASE WHEN plan_type = 'Premium' THEN total_amount ELSE 0 END) AS premium_revenue
FROM subscriptions
GROUP BY DATE_TRUNC('month', subscription_date)
ORDER BY month;
```

**Alternative (using FILTER - PostgreSQL):**
```sql
SELECT
    DATE_TRUNC('month', subscription_date) AS month,
    COUNT(*) FILTER (WHERE plan_type = 'Basic') AS basic_count,
    COUNT(*) FILTER (WHERE plan_type = 'Standard') AS standard_count,
    COUNT(*) FILTER (WHERE plan_type = 'Premium') AS premium_count
FROM subscriptions
GROUP BY DATE_TRUNC('month', subscription_date);
```

---

### Query 33: Cross Join for All Combinations
**Interview Context:** "Generate all possible pairs"  
**Netflix Example:** "Create a user-content recommendation matrix"

```sql
-- Create all possible user-content combinations for recommendation testing
SELECT
    u.user_id,
    u.user_name,
    c.content_id,
    c.title,
    CASE 
        WHEN v.view_id IS NOT NULL THEN 'Already Watched'
        ELSE 'Potential Recommendation'
    END AS recommendation_status
FROM streaming_users u
CROSS JOIN content_catalog c
LEFT JOIN viewing_history v 
    ON u.user_id = v.user_id 
    AND c.content_id = v.content_id
WHERE u.user_id IN (101, 102, 103)  -- Limit for demo
    AND c.content_type = 'Series'
ORDER BY u.user_id, c.title;
```

**Use Cases:**
- Generate all possible combinations
- Fill missing data gaps
- A/B test assignment matrix

**Warning:** CROSS JOIN is expensive (returns m × n rows)

---

## 📅 DATE/TIME ANALYTICS (Queries 34-38)

### Query 34: Cohort Analysis
**Interview Context:** "Group users by signup period, track behavior"  
**Netflix Example:** "Retention by signup month cohort"

```sql
WITH user_cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('month', signup_date) AS cohort_month
    FROM streaming_users
),
user_activity AS (
    SELECT
        uc.cohort_month,
        DATE_TRUNC('month', s.subscription_date) AS activity_month,
        COUNT(DISTINCT s.user_id) AS active_users
    FROM user_cohorts uc
    JOIN subscriptions s ON uc.user_id = s.user_id
    WHERE s.status = 'Active'
    GROUP BY uc.cohort_month, DATE_TRUNC('month', s.subscription_date)
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_month
)
SELECT
    ua.cohort_month,
    cs.cohort_size,
    ua.activity_month,
    ua.active_users,
    ROUND(100.0 * ua.active_users / cs.cohort_size, 2) AS retention_rate,
    EXTRACT(MONTH FROM AGE(ua.activity_month, ua.cohort_month)) AS months_since_signup
FROM user_activity ua
JOIN cohort_sizes cs ON ua.cohort_month = cs.cohort_month
ORDER BY ua.cohort_month, ua.activity_month;
```

**This is THE most common Netflix analytics question**

---

### Query 35: Date Dimension Table (Generate Series)
**Interview Context:** "Fill gaps in time series data"  
**Netflix Example:** "Show revenue for ALL days (even days with zero revenue)"

```sql
WITH date_series AS (
    SELECT generate_series(
        '2021-01-01'::date,
        '2021-12-31'::date,
        '1 day'::interval
    )::date AS date
),
daily_revenue AS (
    SELECT
        subscription_date::date AS date,
        SUM(total_amount) AS revenue
    FROM subscriptions
    GROUP BY subscription_date::date
)
SELECT
    ds.date,
    COALESCE(dr.revenue, 0) AS revenue,
    SUM(COALESCE(dr.revenue, 0)) OVER (ORDER BY ds.date) AS cumulative_revenue
FROM date_series ds
LEFT JOIN daily_revenue dr ON ds.date = dr.date
ORDER BY ds.date;
```

**Why this matters:** Real data has gaps, but reports need complete time series

---

### Query 36: Week-over-Week Growth
**Interview Context:** "Compare to same day last week"  
**Netflix Example:** "WoW signup comparison"

```sql
WITH daily_signups AS (
    SELECT
        signup_date::date AS date,
        COUNT(*) AS signups
    FROM streaming_users
    GROUP BY signup_date::date
)
SELECT
    date,
    signups,
    LAG(signups, 7) OVER (ORDER BY date) AS signups_last_week,
    signups - LAG(signups, 7) OVER (ORDER BY date) AS wow_change,
    ROUND(
        100.0 * (signups - LAG(signups, 7) OVER (ORDER BY date)) 
        / NULLIF(LAG(signups, 7) OVER (ORDER BY date), 0),
        2
    ) AS wow_growth_pct
FROM daily_signups
ORDER BY date;
```

**Pro tip:** Use `NULLIF(denominator, 0)` to avoid division by zero

---

### Query 37: Sessionization - Time-Based Grouping
**Interview Context:** "Define sessions from event stream"  
**Netflix Example:** "Group viewing into binge sessions (gap < 30 min)"

```sql
WITH viewing_with_gaps AS (
    SELECT
        user_id,
        content_id,
        watch_date,
        minutes_watched,
        EXTRACT(EPOCH FROM (
            watch_date - LAG(watch_date) OVER (PARTITION BY user_id ORDER BY watch_date)
        )) / 60 AS minutes_since_last_view
    FROM viewing_history
),
session_starts AS (
    SELECT
        *,
        CASE 
            WHEN minutes_since_last_view IS NULL OR minutes_since_last_view > 30 
            THEN 1 
            ELSE 0 
        END AS is_new_session
    FROM viewing_with_gaps
),
sessions AS (
    SELECT
        *,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY watch_date) AS session_id
    FROM session_starts
)
SELECT
    user_id,
    session_id,
    MIN(watch_date) AS session_start,
    MAX(watch_date) AS session_end,
    COUNT(*) AS items_watched,
    SUM(minutes_watched) AS total_session_minutes
FROM sessions
GROUP BY user_id, session_id
HAVING COUNT(*) >= 2  -- Only multi-item sessions
ORDER BY user_id, session_start;
```

**This pattern is CRITICAL for:**
- User behavior analysis
- Engagement metrics
- Recommendation timing

**Asked at: Netflix, YouTube, Spotify**

---

### Query 38: Business Days Calculation
**Interview Context:** "Exclude weekends/holidays from date calculations"  
**Netflix Example:** "SLA response time (business days only)"

```sql
WITH date_range AS (
    SELECT generate_series(
        '2021-01-01'::date,
        '2021-12-31'::date,
        '1 day'::interval
    )::date AS date
),
business_days AS (
    SELECT
        date,
        EXTRACT(DOW FROM date) AS day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN FALSE  -- Sunday=0, Saturday=6
            ELSE TRUE 
        END AS is_business_day
    FROM date_range
)
SELECT
    date,
    is_business_day,
    SUM(CASE WHEN is_business_day THEN 1 ELSE 0 END) OVER (ORDER BY date) AS business_day_number
FROM business_days
ORDER BY date;
```

**Extension:** Add holiday exclusion logic for complete business day calculation

---

## 🔍 DATA QUALITY & VALIDATION (Queries 39-42)

### Query 39: Duplicate Detection
**Interview Context:** "Find data quality issues"  
**Netflix Example:** "Find users with duplicate email addresses"

```sql
-- Find duplicate emails
SELECT
    email,
    COUNT(*) AS duplicate_count,
    STRING_AGG(user_id::text, ', ') AS affected_user_ids
FROM streaming_users
GROUP BY email
HAVING COUNT(*) > 1;

-- Find duplicate viewing records (same user, content, date)
SELECT
    user_id,
    content_id,
    watch_date,
    COUNT(*) AS duplicate_count
FROM viewing_history
GROUP BY user_id, content_id, watch_date
HAVING COUNT(*) > 1;
```

**Production scenario:** "We have duplicate charges. Find them and calculate refund amount."

---

### Query 40: Data Completeness Check
**Interview Context:** "Validate data pipeline quality"  
**Netflix Example:** "Check for missing critical fields"

```sql
SELECT
    'streaming_users' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN user_name IS NULL THEN 1 ELSE 0 END) AS missing_name,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS missing_country,
    ROUND(100.0 * SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS email_null_rate
FROM streaming_users

UNION ALL

SELECT
    'subscriptions' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS missing_user_id,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS missing_amount,
    SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) AS invalid_amount,
    ROUND(100.0 * SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS invalid_amount_rate
FROM subscriptions;
```

**Interview points:** Shows you think about data quality in production

---

### Query 41: Outlier Detection
**Interview Context:** "Find anomalies in data"  
**Netflix Example:** "Detect unusually high watch times (>24 hours/day)"

```sql
WITH user_daily_watch AS (
    SELECT
        user_id,
        watch_date::date AS date,
        SUM(minutes_watched) AS daily_minutes
    FROM viewing_history
    GROUP BY user_id, watch_date::date
),
stats AS (
    SELECT
        AVG(daily_minutes) AS mean,
        STDDEV(daily_minutes) AS stddev
    FROM user_daily_watch
)
SELECT
    udw.user_id,
    udw.date,
    udw.daily_minutes,
    s.mean,
    s.stddev,
    (udw.daily_minutes - s.mean) / s.stddev AS z_score,
    CASE
        WHEN udw.daily_minutes > 1440 THEN 'IMPOSSIBLE - Exceeds 24 hours'
        WHEN (udw.daily_minutes - s.mean) / s.stddev > 3 THEN 'OUTLIER - 3+ StdDev'
        WHEN (udw.daily_minutes - s.mean) / s.stddev > 2 THEN 'Unusual - 2+ StdDev'
        ELSE 'Normal'
    END AS outlier_flag
FROM user_daily_watch udw
CROSS JOIN stats s
WHERE udw.daily_minutes > 1440 
   OR (udw.daily_minutes - s.mean) / s.stddev > 2
ORDER BY z_score DESC;
```

**Statistical methods:**
- Z-score (standard deviations from mean)
- IQR method (interquartile range)
- Domain logic (can't watch >24 hrs/day)

---

### Query 42: Referential Integrity Check
**Interview Context:** "Find orphaned records"  
**Netflix Example:** "Subscriptions without valid user_id"

```sql
-- Orphaned subscriptions (no matching user)
SELECT
    s.subscription_id,
    s.user_id,
    s.total_amount,
    'Orphaned - No User' AS issue
FROM subscriptions s
LEFT JOIN streaming_users u ON s.user_id = u.user_id
WHERE u.user_id IS NULL

UNION ALL

-- Orphaned viewing history (no matching content)
SELECT
    v.view_id AS subscription_id,
    v.content_id AS user_id,
    v.minutes_watched AS total_amount,
    'Orphaned - No Content' AS issue
FROM viewing_history v
LEFT JOIN content_catalog c ON v.content_id = c.content_id
WHERE c.content_id IS NULL;
```

**Why this matters:** Data integrity issues cause production bugs

---

## ⚡ QUERY OPTIMIZATION (Queries 43-46)

### Query 43: Inefficient vs Optimized - Subquery in SELECT
**Interview Context:** "This query is slow. How would you fix it?"

```sql
-- ❌ INEFFICIENT: Correlated subquery runs once PER ROW
SELECT
    u.user_name,
    (SELECT COUNT(*) FROM subscriptions s WHERE s.user_id = u.user_id) AS sub_count,
    (SELECT SUM(total_amount) FROM subscriptions s WHERE s.user_id = u.user_id) AS total_spent
FROM streaming_users u;

-- ✅ OPTIMIZED: Single join, aggregate once
SELECT
    u.user_name,
    COUNT(s.subscription_id) AS sub_count,
    SUM(s.total_amount) AS total_spent
FROM streaming_users u
LEFT JOIN subscriptions s ON u.user_id = s.user_id
GROUP BY u.user_id, u.user_name;
```

**Performance impact:** 
- Inefficient: O(n²) - runs subquery for each user
- Optimized: O(n) - single table scan + join

**Interview answer:** "I'd refactor the correlated subquery to a JOIN with GROUP BY"

---

### Query 44: EXISTS vs IN - Performance Difference
**Interview Context:** "Which is faster: IN or EXISTS?"

```sql
-- ❌ POTENTIALLY SLOW: IN with large subquery
SELECT *
FROM content_creators
WHERE department_id IN (
    SELECT department_id 
    FROM departments 
    WHERE budget > 10000000
);

-- ✅ USUALLY FASTER: EXISTS (can short-circuit)
SELECT *
FROM content_creators c
WHERE EXISTS (
    SELECT 1 
    FROM departments d 
    WHERE d.department_id = c.department_id 
      AND d.budget > 10000000
);
```

**Why EXISTS is often better:**
- Stops searching after finding first match
- Doesn't materialize full result set
- Better for correlated queries

**Interview answer:** "EXISTS is generally faster because it short-circuits, but modern optimizers often make them equivalent. I'd check EXPLAIN PLAN."

---

### Query 45: Index-Friendly Queries
**Interview Context:** "This query doesn't use indexes. Why?"

```sql
-- ❌ NOT INDEX-FRIENDLY: Function on indexed column
SELECT * FROM streaming_users
WHERE YEAR(signup_date) = 2021;  -- Can't use index on signup_date

-- ✅ INDEX-FRIENDLY: Column comparison
SELECT * FROM streaming_users
WHERE signup_date >= '2021-01-01' 
  AND signup_date < '2022-01-01';

-- ❌ NOT INDEX-FRIENDLY: Wildcard at start
SELECT * FROM streaming_users
WHERE email LIKE '%@gmail.com';  -- Can't use index

-- ✅ INDEX-FRIENDLY: Wildcard at end
SELECT * FROM streaming_users
WHERE email LIKE 'john%';  -- Can use index
```

**Rules for index usage:**
1. Don't wrap indexed column in function
2. Leading wildcards prevent index usage
3. Use SARGable predicates (Search ARGument able)

---

### Query 46: Partition Pruning for Performance
**Interview Context:** "Speed up queries on partitioned tables"

```sql
-- Assume subscriptions table is partitioned by subscription_date (monthly)

-- ❌ SCANS ALL PARTITIONS: No date filter
SELECT COUNT(*)
FROM subscriptions
WHERE plan_type = 'Premium';

-- ✅ PARTITION PRUNING: Only scans relevant partitions
SELECT COUNT(*)
FROM subscriptions
WHERE subscription_date >= '2021-06-01'
  AND subscription_date < '2021-07-01'
  AND plan_type = 'Premium';
```

**Interview tip:** "For large tables, always include partition key in WHERE clause to enable partition pruning"

---

## 🎓 STAFF ENGINEER LEVEL (Queries 47-50)

### Query 47: Incremental Processing Pattern
**Interview Context:** "Design for daily batch processing"  
**Netflix Example:** "Update daily_user_stats table incrementally"

```sql
-- Create incremental daily stats
INSERT INTO daily_user_stats (user_id, stat_date, views, minutes_watched, revenue)
SELECT
    u.user_id,
    CURRENT_DATE - 1 AS stat_date,
    COUNT(v.view_id) AS views,
    SUM(v.minutes_watched) AS minutes_watched,
    SUM(s.total_amount) AS revenue
FROM streaming_users u
LEFT JOIN viewing_history v 
    ON u.user_id = v.user_id 
    AND v.watch_date::date = CURRENT_DATE - 1  -- Yesterday only
LEFT JOIN subscriptions s 
    ON u.user_id = s.user_id 
    AND s.subscription_date::date = CURRENT_DATE - 1
WHERE u.signup_date::date <= CURRENT_DATE - 1  -- Only existing users
GROUP BY u.user_id
ON CONFLICT (user_id, stat_date) 
DO UPDATE SET
    views = EXCLUDED.views,
    minutes_watched = EXCLUDED.minutes_watched,
    revenue = EXCLUDED.revenue;
```

**Why this pattern:**
- Process only new data (efficient)
- Idempotent (can rerun safely)
- Handles late-arriving data

---

### Query 48: Slowly Changing Dimension (SCD Type 2)
**Interview Context:** "Track historical changes in user data"

```sql
-- Track plan changes over time
CREATE TABLE user_plan_history (
    user_id INT,
    plan_type VARCHAR(50),
    price DECIMAL(8,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- Insert new plan change
INSERT INTO user_plan_history (user_id, plan_type, price, valid_from, valid_to, is_current)
VALUES (101, 'Premium', 14.99, '2022-01-01', '9999-12-31', TRUE);

-- When plan changes, close old record and open new one
UPDATE user_plan_history
SET valid_to = '2022-06-30', is_current = FALSE
WHERE user_id = 101 AND is_current = TRUE;

INSERT INTO user_plan_history (user_id, plan_type, price, valid_from, valid_to, is_current)
VALUES (101, 'Standard', 9.99, '2022-07-01', '9999-12-31', TRUE);

-- Query: What was user 101's plan on specific date?
SELECT *
FROM user_plan_history
WHERE user_id = 101
  AND '2022-05-15' BETWEEN valid_from AND valid_to;
```

**Interview gold:** Shows you understand data warehousing concepts

---

### Query 49: Funnel Analysis
**Interview Context:** "Measure conversion through steps"  
**Netflix Example:** "Signup → Trial → Paid conversion funnel"

```sql
WITH funnel_steps AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) AS completed_signup,
        MAX(CASE WHEN event_type = 'trial_start' THEN 1 ELSE 0 END) AS completed_trial,
        MAX(CASE WHEN event_type = 'paid_conversion' THEN 1 ELSE 0 END) AS completed_paid
    FROM user_events
    WHERE event_date >= '2021-01-01'
    GROUP BY user_id
)
SELECT
    SUM(completed_signup) AS step1_signup,
    SUM(completed_trial) AS step2_trial,
    SUM(completed_paid) AS step3_paid,
    ROUND(100.0 * SUM(completed_trial) / NULLIF(SUM(completed_signup), 0), 2) AS signup_to_trial_pct,
    ROUND(100.0 * SUM(completed_paid) / NULLIF(SUM(completed_trial), 0), 2) AS trial_to_paid_pct,
    ROUND(100.0 * SUM(completed_paid) / NULLIF(SUM(completed_signup), 0), 2) AS overall_conversion_pct
FROM funnel_steps;
```

**Variation:** Time-based funnel (must complete within X days)

---

### Query 50: A/B Test Statistical Analysis
**Interview Context:** "Evaluate A/B test results"  
**Netflix Example:** "Did new recommendation algo increase watch time?"

```sql
WITH test_groups AS (
    SELECT
        user_id,
        test_variant,  -- 'control' or 'treatment'
        SUM(minutes_watched) AS total_minutes
    FROM ab_test_assignments a
    JOIN viewing_history v ON a.user_id = v.user_id
    WHERE test_start_date <= v.watch_date 
      AND v.watch_date <= test_end_date
    GROUP BY user_id, test_variant
),
summary_stats AS (
    SELECT
        test_variant,
        COUNT(*) AS user_count,
        AVG(total_minutes) AS avg_minutes,
        STDDEV(total_minutes) AS stddev_minutes
    FROM test_groups
    GROUP BY test_variant
)
SELECT
    *,
    -- Calculate effect size (Cohen's d)
    (
        (SELECT avg_minutes FROM summary_stats WHERE test_variant = 'treatment') -
        (SELECT avg_minutes FROM summary_stats WHERE test_variant = 'control')
    ) / SQRT(
        (POWER((SELECT stddev_minutes FROM summary_stats WHERE test_variant = 'treatment'), 2) +
         POWER((SELECT stddev_minutes FROM summary_stats WHERE test_variant = 'control'), 2)) / 2
    ) AS cohens_d_effect_size
FROM summary_stats;
```

**Interview answer:** "Cohen's d > 0.5 indicates medium effect, > 0.8 large effect"

---

## 🐛 PRODUCTION DEBUGGING SCENARIOS

### Scenario 1: Sudden Revenue Drop
**Problem:** "Revenue dropped 20% yesterday. Find the cause."

```sql
-- Step 1: Compare yesterday vs same day last week
WITH daily_revenue AS (
    SELECT
        subscription_date::date AS date,
        COUNT(*) AS subscription_count,
        SUM(total_amount) AS revenue,
        COUNT(CASE WHEN plan_type = 'Premium' THEN 1 END) AS premium_count,
        COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) AS cancelled_count
    FROM subscriptions
    WHERE subscription_date::date IN (CURRENT_DATE - 1, CURRENT_DATE - 8)
    GROUP BY subscription_date::date
)
SELECT *,
    revenue - LAG(revenue) OVER (ORDER BY date) AS revenue_change,
    subscription_count - LAG(subscription_count) OVER (ORDER BY date) AS sub_count_change
FROM daily_revenue;

-- Step 2: Check for data pipeline issues
SELECT
    subscription_date::date,
    COUNT(*) AS records,
    MIN(created_at) AS first_record_time,
    MAX(created_at) AS last_record_time
FROM subscriptions
WHERE subscription_date::date = CURRENT_DATE - 1
GROUP BY subscription_date::date;
```

---

### Scenario 2: Duplicate Charges Investigation
**Problem:** "Users reporting duplicate charges"

```sql
-- Find potential duplicates (same user, amount, within 5 minutes)
WITH potential_duplicates AS (
    SELECT
        s1.user_id,
        s1.subscription_id AS id1,
        s2.subscription_id AS id2,
        s1.total_amount,
        s1.subscription_date AS time1,
        s2.subscription_date AS time2,
        EXTRACT(EPOCH FROM (s2.subscription_date - s1.subscription_date)) / 60 AS minutes_apart
    FROM subscriptions s1
    JOIN subscriptions s2 
        ON s1.user_id = s2.user_id
        AND s1.total_amount = s2.total_amount
        AND s1.subscription_id < s2.subscription_id
        AND s2.subscription_date - s1.subscription_date < INTERVAL '5 minutes'
)
SELECT
    u.user_name,
    u.email,
    pd.*
FROM potential_duplicates pd
JOIN streaming_users u ON pd.user_id = u.user_id
ORDER BY pd.user_id, time1;
```

---

### Scenario 3: Slow Dashboard Query
**Problem:** "User engagement dashboard times out"

```sql
-- Before optimization: Multiple correlated subqueries (SLOW)
-- After optimization: Single query with window functions

-- Optimized version
WITH user_metrics AS (
    SELECT
        u.user_id,
        u.user_name,
        COUNT(DISTINCT v.view_id) AS total_views,
        SUM(v.minutes_watched) AS total_minutes,
        COUNT(DISTINCT v.watch_date::date) AS active_days,
        MAX(v.watch_date) AS last_activity,
        RANK() OVER (ORDER BY SUM(v.minutes_watched) DESC) AS engagement_rank,
        NTILE(10) OVER (ORDER BY SUM(v.minutes_watched) DESC) AS engagement_decile
    FROM streaming_users u
    LEFT JOIN viewing_history v ON u.user_id = v.user_id
    WHERE u.signup_date >= CURRENT_DATE - 90  -- Last 90 days only
    GROUP BY u.user_id, u.user_name
)
SELECT * FROM user_metrics
WHERE total_views > 0  -- Filter before ranking if possible
ORDER BY engagement_rank
LIMIT 100;

-- Add covering index:
-- CREATE INDEX idx_viewing_user_date ON viewing_history(user_id, watch_date, minutes_watched);
```

---

## 🎯 PERFORMANCE TUNING GUIDE

### 1. Use EXPLAIN ANALYZE
```sql
EXPLAIN ANALYZE
SELECT u.user_name, COUNT(v.view_id)
FROM streaming_users u
JOIN viewing_history v ON u.user_id = v.user_id
GROUP BY u.user_id, u.user_name;
```

**What to look for:**
- Seq Scan → needs index
- High cost numbers
- Many rows filtered out

---

### 2. Materialized Views for Complex Aggregations
```sql
CREATE MATERIALIZED VIEW daily_user_stats AS
SELECT
    user_id,
    watch_date::date AS date,
    COUNT(*) AS views,
    SUM(minutes_watched) AS total_minutes
FROM viewing_history
GROUP BY user_id, watch_date::date;

-- Refresh periodically
REFRESH MATERIALIZED VIEW daily_user_stats;

-- Query is now instant
SELECT * FROM daily_user_stats WHERE user_id = 101;
```

---

### 3. Partial Indexes for Specific Queries
```sql
-- Only index active subscriptions (smaller, faster)
CREATE INDEX idx_active_subscriptions 
ON subscriptions(user_id, subscription_date)
WHERE status = 'Active';

-- Query uses this index
SELECT * FROM subscriptions
WHERE user_id = 101 AND status = 'Active';
```

---

### 4. Query Hints (Database-Specific)
```sql
-- PostgreSQL: Force index usage
SELECT * FROM viewing_history
WHERE user_id = 101
ORDER BY watch_date DESC
LIMIT 10;
-- Hint: /*+ IndexScan(viewing_history idx_user_date) */
```

---

## 📊 STUDY PLAN - PART 2

### Week 5: Advanced Window Functions
- **Day 1:** LAG/LEAD, moving averages
- **Day 2:** FIRST_VALUE/LAST_VALUE, NTILE
- **Day 3:** PERCENT_RANK, CUME_DIST
- **Day 4:** Window frames (ROWS vs RANGE)
- **Day 5:** Practice 20 window function problems

### Week 6: Complex Patterns
- **Day 1:** Self-joins and pair problems
- **Day 2:** Gap and Island problem
- **Day 3:** Sessionization
- **Day 4:** Cohort analysis
- **Day 5:** Funnel analysis

### Week 7: Optimization & Production
- **Day 1:** Read execution plans
- **Day 2:** Index optimization
- **Day 3:** Query refactoring
- **Day 4:** Data quality checks
- **Day 5:** Debug production scenarios

### Week 8: Staff Engineer Prep
- **Day 1-2:** SCD implementation
- **Day 3:** A/B test analysis
- **Day 4:** Incremental processing
- **Day 5:** System design with SQL

---

## 🏆 PART 2 FINAL CHECKLIST

### Advanced Skills
- [ ] Can you write moving averages with custom frames?
- [ ] Can you solve gap and island problems?
- [ ] Can you implement sessionization logic?
- [ ] Can you build cohort analysis from scratch?
- [ ] Can you detect and handle outliers?

### Performance
- [ ] Can you read and interpret EXPLAIN PLAN?
- [ ] Do you know when to use indexes?
- [ ] Can you refactor correlated subqueries?
- [ ] Do you understand partition pruning?
- [ ] Can you optimize slow queries?

### Production
- [ ] Can you write idempotent data pipelines?
- [ ] Do you validate data quality?
- [ ] Can you debug revenue discrepancies?
- [ ] Do you handle late-arriving data?
- [ ] Can you implement SCD patterns?

### System Design
- [ ] Can you design a data model for recommendations?
- [ ] Do you understand fact vs dimension tables?
- [ ] Can you explain partitioning strategies?
- [ ] Do you know when to denormalize?
- [ ] Can you design for scale (billions of rows)?

---

## 🎯 COMBINED MASTERY (Part 1 + Part 2)

**Part 1:** Handles 95% of Netflix interviews  
**Part 2:** Makes you the interviewer's dream hire

**Combined, you can:**
✅ Pass ANY SQL interview at Netflix, Meta, Google  
✅ Design production data pipelines  
✅ Debug complex performance issues  
✅ Lead technical discussions on data architecture  
✅ Command $200k+ salaries with confidence

---

## 💡 Final Thoughts

**Part 1** taught you to solve problems.  
**Part 2** taught you to solve problems *efficiently* and *at scale*.

The difference between Senior and Staff Engineer is not just *what* you know, but *how* you think about:
- Performance
- Data quality
- Production operations
- System design
- Trade-offs

**Master Part 2 → You're not just interview-ready, you're production-ready.**

---

**Now go crush that Netflix interview! 🚀**

*You have the skills. You have the patterns. You have the confidence.*
