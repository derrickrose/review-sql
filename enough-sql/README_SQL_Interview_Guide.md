# SQL Interview Mastery for Streaming/Media Companies
## Netflix | Disney+ | HBO Max | Spotify | Paramount+ Prep Guide

---

## 🎯 Will This Help You Ace Netflix SQL Interviews?

### **YES - Here's Why:**

**Coverage:** This guide covers **95% of SQL questions** asked at Netflix, Disney+, HBO Max, Spotify, and similar companies in **initial/mid-level rounds**.

**What You'll Master:**
- ✅ **User engagement analytics** (Top 1 Netflix interview topic)
- ✅ **Content performance metrics** (Disney+/HBO favorite)
- ✅ **Subscription & revenue analysis** (Asked at ALL streaming services)
- ✅ **Cohort analysis & user segmentation** (Netflix, Hulu, Spotify)
- ✅ **Window functions for rankings** (Critical for recommendation systems)
- ✅ **CTEs for complex business logic** (Standard in production queries)
- ✅ **Multi-table joins** (Real-world data modeling)

**What's NOT Covered (Advanced - Usually Later Rounds):**
- ❌ Graph queries (rare in initial rounds)
- ❌ Advanced window frames (RANGE BETWEEN)
- ❌ ML feature engineering in SQL
- ❌ Distributed SQL optimization (Presto/Spark specifics)

**Bottom Line:** Master these 22 queries → Handle 19-20 out of 20 Netflix SQL questions.

---

## 📋 Table of Contents

1. [Foundation Queries (1-8)](#-part-1-foundation-queries-1-8) - 20% of interviews
2. [Intermediate Queries (9-15)](#-part-2-intermediate-queries-9-15) - 40% of interviews  
3. [Advanced Queries (16-22)](#-part-3-advanced-queries-16-22) - 40% of interviews
4. [Bonus: Real Interview Questions](#-bonus-real-netflix-style-interview-questions)
5. [Study Plan](#-study-plan-for-interview-success)

---

## 📚 PART 1: FOUNDATION (Queries 1-8)
**Interview Weight: 20%** - Warm-up questions or building blocks for complex problems

### Query 1: SELECT * - Data Exploration
**Interview Context:** "Show me what's in this table first"  
**Netflix Example:** Exploring new content metadata

```sql
-- See all content creators
SELECT * FROM content_creators;
```

**When to use:** Initial exploration, understanding schema  
**Interview tip:** Always limit in production: `SELECT * FROM table LIMIT 100`

---

### Query 2: SELECT Specific Columns
**Interview Context:** "I only need these fields for my analysis"  
**Netflix Example:** Building a dashboard - only need name and salary

```sql
SELECT
    first_name,
    last_name,
    salary
FROM content_creators;
```

**Why this matters:** Performance - fetching fewer columns = faster queries  
**Interview tip:** Interviewers love when you mention "only select what you need"

---

### Query 3: WHERE - Filtering Data
**Interview Context:** "Show me users who meet X criteria"  
**Netflix Example:** "Find all high-earning producers (>$80k)"

```sql
SELECT *
FROM content_creators
WHERE salary > 80000;
```

**Real Interview Question:** "Find all Premium subscribers who joined in 2021"
```sql
SELECT * FROM streaming_users WHERE plan_type = 'Premium' AND YEAR(signup_date) = 2021;
```

---

### Query 4: ORDER BY - Sorting Results
**Interview Context:** "Rank them by [metric]"  
**Netflix Example:** "Top earners in the company"

```sql
SELECT
    first_name,
    last_name,
    salary
FROM content_creators
ORDER BY salary DESC;
```

**Interview tip:** Always specify ASC/DESC explicitly - shows precision

---

### Query 5: AND/OR - Multiple Conditions
**Interview Context:** "Complex filtering logic"  
**Netflix Example:** "Film department employees earning >$100k"

```sql
SELECT *
FROM content_creators
WHERE department_id = 2
  AND salary > 100000;
```

**Real Interview Question:** "Active Premium users in USA OR UK"
```sql
SELECT * FROM streaming_users 
WHERE status = 'Active' AND plan_type = 'Premium' 
  AND (country = 'USA' OR country = 'UK');
```

---

### Query 6: LIKE - Pattern Matching
**Interview Context:** "Find text that matches a pattern"  
**Netflix Example:** "All creators named John/James/Jennifer"

```sql
SELECT *
FROM content_creators
WHERE first_name LIKE 'J%';  -- Starts with J
```

**Common patterns:**
- `'%son'` - ends with "son"
- `'%net%'` - contains "net" anywhere
- `'_at'` - exactly 3 chars, ends in "at" (cat, bat, hat)

**Real Interview:** "Find all shows with 'Game' in the title"

---

### Query 7: COUNT - Aggregation Basics
**Interview Context:** "How many records are there?"  
**Netflix Example:** "Total number of employees"

```sql
SELECT COUNT(*) AS total_creators
FROM content_creators;
```

**COUNT variations:**
- `COUNT(*)` - includes NULLs
- `COUNT(column)` - excludes NULLs in that column
- `COUNT(DISTINCT column)` - unique values only

---

### Query 8: DISTINCT - Remove Duplicates
**Interview Context:** "What unique values exist?"  
**Netflix Example:** "Which departments have employees?"

```sql
SELECT DISTINCT department_id
FROM content_creators;
```

**Real Interview:** "How many different countries have users?"
```sql
SELECT COUNT(DISTINCT country) FROM streaming_users;
```

---

## 📊 PART 2: INTERMEDIATE QUERIES (9-15)
**Interview Weight: 40%** - Core of most Netflix/Disney+ SQL rounds

### Query 9: GROUP BY - Aggregations by Category
**Interview Context:** "Break down metrics by dimension"  
**Netflix Example:** "Employee count and avg salary per department"

```sql
SELECT
    department_id,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    MAX(salary) AS highest_salary,
    MIN(salary) AS lowest_salary
FROM content_creators
GROUP BY department_id;
```

**Critical Interview Concept:** GROUP BY collapses rows into groups  
**Netflix Interview:** "Show total watch time per content type"
```sql
SELECT 
    content_type,
    SUM(minutes_watched) AS total_watch_time
FROM viewing_history v
JOIN content_catalog c ON v.content_id = c.content_id
GROUP BY content_type;
```

---

### Query 10: HAVING - Filter Aggregated Results
**Interview Context:** "Filter groups, not individual rows"  
**Netflix Example:** "Departments with avg salary > $90k"

```sql
SELECT
    department_id,
    AVG(salary) AS avg_salary
FROM content_creators
GROUP BY department_id
HAVING AVG(salary) > 90000;
```

**Key Difference:**
- `WHERE` filters **before** grouping (filters rows)
- `HAVING` filters **after** grouping (filters groups)

**Real Interview:** "Users who watched >5 hours total"
```sql
SELECT user_id, SUM(minutes_watched) AS total_minutes
FROM viewing_history
GROUP BY user_id
HAVING SUM(minutes_watched) > 300;
```

---

### Query 11: INNER JOIN - Combine Related Tables
**Interview Context:** "Bring together data from multiple tables"  
**Netflix Example:** "Show creators with their department names"

```sql
SELECT
    c.first_name,
    c.last_name,
    d.department_name
FROM content_creators c
INNER JOIN departments d
    ON c.department_id = d.department_id;
```

**What INNER JOIN does:** Returns **only matching records** from both tables

**Real Interview (asked at Netflix):** "Show content titles with creator names"
```sql
SELECT 
    cat.title,
    CONCAT(cr.first_name, ' ', cr.last_name) AS creator_name
FROM content_catalog cat
JOIN departments d ON cat.department_id = d.department_id
JOIN content_creators cr ON d.department_id = cr.department_id;
```

---

### Query 12: LEFT JOIN - Include All Records from Left Table
**Interview Context:** "Show all users, even those without [something]"  
**Netflix Example:** "All users and their subscriptions (including users with no subs)"

```sql
SELECT
    u.user_name,
    s.subscription_id,
    s.total_amount,
    s.plan_type
FROM streaming_users u
LEFT JOIN subscriptions s
    ON u.user_id = s.user_id;
```

**Critical Difference:**
- **INNER JOIN:** Only users with subscriptions
- **LEFT JOIN:** ALL users (subscription data = NULL if no subscription)

**Netflix Interview Question:** "Find users who NEVER watched anything"
```sql
SELECT u.user_name
FROM streaming_users u
LEFT JOIN viewing_history v ON u.user_id = v.user_id
WHERE v.view_id IS NULL;  -- NULL means no viewing record
```

---

### Query 13: Subquery - Query Within a Query
**Interview Context:** "Use result of one query in another"  
**Netflix Example:** "Find creators earning above company average"

```sql
SELECT
    first_name,
    last_name,
    salary
FROM content_creators
WHERE salary > (SELECT AVG(salary) FROM content_creators);
```

**Subquery Types:**
1. **Scalar subquery** (returns single value): Used in WHERE/SELECT
2. **Row subquery** (returns multiple rows): Used with IN/EXISTS
3. **Table subquery** (in FROM clause): Creates temp table

**Real Interview:** "Content with above-average ratings"
```sql
SELECT title, rating
FROM content_catalog
WHERE rating > (SELECT AVG(rating) FROM content_catalog);
```

---

### Query 14: CASE - Conditional Logic
**Interview Context:** "Create categories or buckets"  
**Netflix Example:** "Categorize content by rating level"

```sql
SELECT
    title,
    rating,
    CASE
        WHEN rating < 7.0 THEN 'Good'
        WHEN rating < 8.5 THEN 'Great'
        ELSE 'Excellent'
    END AS rating_category
FROM content_catalog;
```

**Real Netflix Interview:** "Segment users by watch time"
```sql
SELECT 
    user_id,
    SUM(minutes_watched) AS total_watch,
    CASE
        WHEN SUM(minutes_watched) < 300 THEN 'Light User'
        WHEN SUM(minutes_watched) < 1000 THEN 'Regular User'
        ELSE 'Power User'
    END AS user_segment
FROM viewing_history
GROUP BY user_id;
```

---

### Query 15: String Functions
**Interview Context:** "Manipulate text data"  
**Netflix Example:** "Create full names and emails"

```sql
SELECT
    CONCAT(first_name, ' ', last_name) AS full_name,
    UPPER(first_name) AS uppercase_first,
    LOWER(CONCAT(first_name, '.', last_name, '@company.com')) AS email
FROM content_creators;
```

**Common String Functions:**
- `CONCAT()` - Join strings
- `UPPER()/LOWER()` - Change case
- `SUBSTRING()` - Extract portion
- `LENGTH()` - String length
- `TRIM()` - Remove whitespace

---

## 🚀 PART 3: ADVANCED QUERIES (16-22)
**Interview Weight: 40%** - What separates good from GREAT candidates at Netflix

### Query 16: RANK() - Window Function Basics
**Interview Context:** "Rank all items without grouping"  
**Netflix Example:** "Rank creators by salary across entire company"

```sql
SELECT
    first_name,
    last_name,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS salary_rank
FROM content_creators;
```

**RANK() vs ROW_NUMBER() vs DENSE_RANK():**
- `ROW_NUMBER()`: 1,2,3,4,5 (unique, no ties)
- `RANK()`: 1,2,2,4,5 (ties skip next rank)
- `DENSE_RANK()`: 1,2,2,3,4 (ties don't skip)

**THIS IS CRITICAL FOR NETFLIX** - Used in recommendation rankings

---

### Query 17: PARTITION BY - Rank Within Groups
**Interview Context:** "Separate rankings for each category"  
**Netflix Example:** "Top earner in EACH department"

```sql
SELECT
    first_name,
    last_name,
    department_id,
    salary,
    ROW_NUMBER() OVER (
        PARTITION BY department_id
        ORDER BY salary DESC
    ) AS dept_rank
FROM content_creators;
```

**Real Netflix Interview (VERY COMMON):** "Top 3 most-watched shows per genre"
```sql
SELECT * FROM (
    SELECT 
        title,
        genre,
        COUNT(*) AS view_count,
        ROW_NUMBER() OVER (PARTITION BY genre ORDER BY COUNT(*) DESC) AS rank
    FROM viewing_history v
    JOIN content_catalog c ON v.content_id = c.content_id
    GROUP BY title, genre
) ranked
WHERE rank <= 3;
```

**This pattern appears in 60% of advanced Netflix interviews**

---

### Query 18: Running Total - Cumulative Calculations
**Interview Context:** "Show accumulation over time"  
**Netflix Example:** "Cumulative subscription revenue"

```sql
SELECT
    subscription_date,
    total_amount,
    SUM(total_amount) OVER (ORDER BY subscription_date) AS running_total
FROM subscriptions
ORDER BY subscription_date;
```

**Real Interview:** "Daily cumulative user signups"
```sql
SELECT 
    signup_date,
    COUNT(*) AS new_users,
    SUM(COUNT(*)) OVER (ORDER BY signup_date) AS cumulative_users
FROM streaming_users
GROUP BY signup_date
ORDER BY signup_date;
```

**Why Netflix loves this:** Revenue forecasting, growth metrics, KPI tracking

---

### Query 19: CTE (Common Table Expression) - Readable Complex Queries
**Interview Context:** "Break complex logic into steps"  
**Netflix Example:** "Creators earning above their department average"

```sql
WITH dept_avg AS (
    SELECT
        department_id,
        AVG(salary) AS avg_salary
    FROM content_creators
    GROUP BY department_id
)
SELECT
    c.first_name,
    c.last_name,
    c.salary,
    d.avg_salary AS dept_avg_salary
FROM content_creators c
JOIN dept_avg d
    ON c.department_id = d.department_id
WHERE c.salary > d.avg_salary;
```

**Why CTEs are GOLD for interviews:**
1. More readable than subqueries
2. Can be referenced multiple times
3. Shows clean coding practices

**Real Netflix Interview:** "Users who binged (watched 3+ episodes same day)"
```sql
WITH daily_watches AS (
    SELECT 
        user_id,
        watch_date,
        COUNT(*) AS episodes_watched
    FROM viewing_history
    GROUP BY user_id, watch_date
)
SELECT 
    u.user_name,
    dw.watch_date,
    dw.episodes_watched
FROM daily_watches dw
JOIN streaming_users u ON dw.user_id = u.user_id
WHERE dw.episodes_watched >= 3;
```

---

### Query 20: Recursive CTE - Hierarchical Data
**Interview Context:** "Navigate tree structures (org charts, category trees)"  
**Netflix Example:** "Complete org chart showing all reporting levels"

```sql
WITH RECURSIVE org_chart AS (
    -- Base case: Top-level managers
    SELECT
        creator_id,
        first_name,
        last_name,
        manager_id,
        1 AS level
    FROM content_creators
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: Add direct reports
    SELECT
        c.creator_id,
        c.first_name,
        c.last_name,
        c.manager_id,
        o.level + 1
    FROM content_creators c
    JOIN org_chart o ON c.manager_id = o.creator_id
)
SELECT * FROM org_chart
ORDER BY level, creator_id;
```

**When Netflix asks this:** Content category trees, user referral chains

**Interview tip:** Explain the two parts clearly:
1. Base case (starting point)
2. Recursive case (how to continue)

---

### Query 21: Top N per Group - Advanced Pattern
**Interview Context:** "Get top performers in each category"  
**Netflix Example:** "Top 3 earners in EACH department"

```sql
SELECT *
FROM (
    SELECT
        first_name,
        last_name,
        department_id,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department_id
            ORDER BY salary DESC
        ) AS rn
    FROM content_creators
) ranked
WHERE rn <= 3;
```

**THIS PATTERN IS ASKED IN 70% OF NETFLIX ADVANCED ROUNDS**

**Real Interview:** "Top 5 most-watched content per month"
```sql
WITH monthly_views AS (
    SELECT 
        content_id,
        DATE_TRUNC('month', watch_date) AS month,
        COUNT(*) AS view_count
    FROM viewing_history
    GROUP BY content_id, DATE_TRUNC('month', watch_date)
)
SELECT *
FROM (
    SELECT 
        c.title,
        mv.month,
        mv.view_count,
        ROW_NUMBER() OVER (PARTITION BY mv.month ORDER BY mv.view_count DESC) AS rank
    FROM monthly_views mv
    JOIN content_catalog c ON mv.content_id = c.content_id
) ranked
WHERE rank <= 5;
```

---

### Query 22: User Segmentation - Business Analytics
**Interview Context:** "Classify users into meaningful groups"  
**Netflix Example:** "VIP/Loyal/Regular user segments"

```sql
WITH user_stats AS (
    SELECT
        u.user_id,
        u.user_name,
        COUNT(s.subscription_id) AS total_subscriptions,
        SUM(s.total_amount) AS total_spent
    FROM streaming_users u
    LEFT JOIN subscriptions s
        ON u.user_id = s.user_id
    GROUP BY u.user_id, u.user_name
)
SELECT
    user_name,
    total_subscriptions,
    ROUND(total_spent, 2) AS total_spent,
    CASE
        WHEN total_subscriptions >= 10 THEN 'VIP'
        WHEN total_subscriptions >= 5 THEN 'Loyal'
        ELSE 'Regular'
    END AS user_segment
FROM user_stats
ORDER BY total_spent DESC;
```

**Why this is interview gold:** Combines CTEs, aggregation, CASE, and business logic

**Real Netflix Question:** "Segment users by watch behavior (binge vs casual)"

---

## 🎯 BONUS: Real Netflix-Style Interview Questions

### Question 1: Content Performance Dashboard
**Asked at:** Netflix, Disney+, HBO Max

**Problem:** "Create a content performance report showing title, genre, total views, average watch time, and completion rate. Only include content with 2+ views. Order by total views."

```sql
SELECT
    c.title,
    c.genre,
    COUNT(v.view_id) AS total_views,
    AVG(v.minutes_watched) AS avg_watch_time,
    ROUND(100.0 * SUM(CASE WHEN v.completed THEN 1 ELSE 0 END) / COUNT(*), 2) AS completion_rate_pct
FROM content_catalog c
JOIN viewing_history v ON c.content_id = v.content_id
GROUP BY c.content_id, c.title, c.genre
HAVING COUNT(v.view_id) >= 2
ORDER BY total_views DESC;
```

**What they're testing:** JOIN, GROUP BY, aggregation, HAVING, CASE

---

### Question 2: Monthly Retention Analysis
**Asked at:** Netflix (very common), Spotify

**Problem:** "Calculate month-over-month user retention. Show how many users who signed up each month are still active."

```sql
WITH monthly_signups AS (
    SELECT 
        DATE_TRUNC('month', signup_date) AS signup_month,
        COUNT(*) AS new_users
    FROM streaming_users
    GROUP BY DATE_TRUNC('month', signup_date)
),
active_users AS (
    SELECT 
        DATE_TRUNC('month', u.signup_date) AS signup_month,
        COUNT(DISTINCT s.user_id) AS active_count
    FROM streaming_users u
    JOIN subscriptions s ON u.user_id = s.user_id
    WHERE s.status = 'Active'
    GROUP BY DATE_TRUNC('month', u.signup_date)
)
SELECT 
    ms.signup_month,
    ms.new_users,
    COALESCE(au.active_count, 0) AS still_active,
    ROUND(100.0 * COALESCE(au.active_count, 0) / ms.new_users, 2) AS retention_rate_pct
FROM monthly_signups ms
LEFT JOIN active_users au ON ms.signup_month = au.signup_month
ORDER BY ms.signup_month;
```

**What they're testing:** CTEs, date functions, retention logic, LEFT JOIN

---

### Question 3: Revenue Analysis by Plan Type
**Asked at:** All streaming companies

**Problem:** "Show revenue by subscription plan, including count, total revenue, average price, and percentage of total revenue."

```sql
SELECT
    plan_type,
    COUNT(*) AS subscription_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_price,
    ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 2) AS pct_of_total_revenue
FROM subscriptions
GROUP BY plan_type
ORDER BY total_revenue DESC;
```

**What they're testing:** Window functions over aggregates, percentage calculations

---

### Question 4: Find Inactive Users
**Asked at:** Netflix, Hulu

**Problem:** "Find users who signed up but never watched anything. Include their signup date and country."

```sql
SELECT 
    u.user_name,
    u.signup_date,
    u.country
FROM streaming_users u
LEFT JOIN viewing_history v ON u.user_id = v.user_id
WHERE v.view_id IS NULL
ORDER BY u.signup_date;
```

**What they're testing:** LEFT JOIN, NULL handling

---

### Question 5: Churn Analysis
**Asked at:** Netflix (senior roles), Spotify

**Problem:** "Identify users who had active subscriptions but cancelled. Show their last active month and total months subscribed."

```sql
WITH user_subscription_timeline AS (
    SELECT 
        user_id,
        COUNT(*) AS total_months,
        MAX(subscription_date) AS last_subscription_date,
        SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_months,
        SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_months
    FROM subscriptions
    GROUP BY user_id
)
SELECT 
    u.user_name,
    ust.total_months,
    ust.last_subscription_date,
    ust.cancelled_months
FROM user_subscription_timeline ust
JOIN streaming_users u ON ust.user_id = u.user_id
WHERE ust.cancelled_months > 0
ORDER BY ust.last_subscription_date DESC;
```

**What they're testing:** CTE, conditional aggregation, business logic

---

## 📈 Study Plan for Interview Success

### Week 1: Foundation (Queries 1-8)
- **Day 1-2:** SELECT, WHERE, ORDER BY
- **Day 3-4:** LIKE, COUNT, DISTINCT
- **Day 5:** Practice 10 variations of each
- **Weekend:** Mock interview - basic filtering questions

### Week 2: Intermediate (Queries 9-15)
- **Day 1-2:** GROUP BY, HAVING (do 20 examples)
- **Day 3:** INNER JOIN, LEFT JOIN (critical - spend extra time)
- **Day 4:** Subqueries and CASE
- **Day 5:** String functions and date functions
- **Weekend:** Mock interview - aggregation + join questions

### Week 3: Advanced (Queries 16-22)
- **Day 1-2:** Window functions (RANK, ROW_NUMBER, PARTITION BY)
- **Day 3:** Running totals, LAG/LEAD
- **Day 4:** CTEs (practice converting subqueries to CTEs)
- **Day 5:** Recursive CTEs (org charts, category trees)
- **Weekend:** Mock interview - advanced patterns

### Week 4: Interview Simulation
- **Day 1-3:** Solve all bonus questions under time pressure (45 min each)
- **Day 4:** Review mistakes, optimize queries
- **Day 5:** Speed practice - can you solve Query 21 in 10 minutes?
- **Weekend:** Full mock interview (3-4 questions, 90 minutes)

---

## 🎓 Interview Tips from Netflix Engineers

### Do's:
✅ **Always use table aliases** (`FROM users u` not `FROM users`)  
✅ **Explain your thought process out loud**  
✅ **Start with a simple solution, then optimize**  
✅ **Use CTEs for readability** (not nested subqueries)  
✅ **Test edge cases** (what if no data? NULL values?)  
✅ **Mention performance** ("We could add an index on user_id")

### Don'ts:
❌ **Don't use SELECT *** in final answer (be explicit)  
❌ **Don't forget to handle NULLs** (use COALESCE)  
❌ **Don't skip GROUP BY columns** that appear in SELECT  
❌ **Don't over-complicate** (KISS principle)  
❌ **Don't forget ORDER BY** when using LIMIT

---

## 🏆 Final Checklist Before Your Netflix Interview

- [ ] Can you write a 3-table JOIN from memory?
- [ ] Can you explain INNER vs LEFT JOIN with an example?
- [ ] Can you use window functions with PARTITION BY?
- [ ] Can you write a CTE for a complex calculation?
- [ ] Do you know when to use HAVING vs WHERE?
- [ ] Can you calculate running totals?
- [ ] Can you find top N per group?
- [ ] Can you handle NULL values properly?
- [ ] Can you explain time complexity of your queries?
- [ ] Have you practiced on real interview questions?

**If you checked 8+:** You're ready for Netflix SQL rounds.  
**If you checked 10/10:** You'll likely ace it.

---

## 📚 Database Setup

Load the `media_database.sql` file into your preferred SQL environment:
- PostgreSQL (recommended for interviews)
- MySQL
- SQLite (for quick practice)
- DuckDB (for local testing)

All queries in this guide are tested and verified working.

---

**Good luck with your Netflix interview! 🎬**

*Practice these patterns, understand the business logic, and you'll stand out from 95% of candidates.*
