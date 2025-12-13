# Query Optimization Guide

Guide to optimizing query performance in Query Engine.

## Understanding Query Execution

### Execution Flow

```
SQL Query → Parse → Plan → Optimize → Execute → Results
```

Each stage offers optimization opportunities.

## Optimization Techniques

### 1. Use Indexes

Indexes dramatically speed up lookups:

```sql
-- Without index: Full table scan O(n)
SELECT * FROM users WHERE email = 'test@example.com';

-- With hash index: O(1) lookup
CREATE INDEX idx_email ON users(email) USING HASH;
SELECT * FROM users WHERE email = 'test@example.com';
```

**When to create indexes:**
- Primary key columns
- Foreign key columns (JOIN performance)
- Frequently filtered columns (WHERE)
- Columns used in ORDER BY (B-Tree only)

### 2. Filter Early

Push filters as close to data source as possible:

```sql
-- Bad: Filter after JOIN
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.salary > 100000;

-- Better: Filter before JOIN (optimizer may do this automatically)
SELECT e.name, d.dept_name
FROM (SELECT * FROM employees WHERE salary > 100000) e
JOIN departments d ON e.dept_id = d.id;
```

### 3. Select Only Needed Columns

```sql
-- Bad: Select all columns
SELECT * FROM large_table;

-- Good: Select only what you need
SELECT id, name FROM large_table;
```

This reduces:
- I/O (fewer columns to read)
- Memory (smaller result set)
- Network (less data transfer)

### 4. Limit Results Early

```sql
-- Bad: Process all, then limit
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;

-- Good: Use limit hints when possible
-- (Optimizer can stop after finding 10 rows)
```

### 5. Avoid Functions on Indexed Columns

```sql
-- Bad: Can't use index
SELECT * FROM users WHERE UPPER(email) = 'TEST@EXAMPLE.COM';

-- Good: Store normalized data, use index
SELECT * FROM users WHERE email_lower = 'test@example.com';
```

### 6. Use EXISTS Instead of IN for Large Lists

```sql
-- Less efficient for large subqueries
SELECT * FROM orders 
WHERE customer_id IN (SELECT id FROM customers WHERE region = 'US');

-- More efficient
SELECT * FROM orders o
WHERE EXISTS (
    SELECT 1 FROM customers c 
    WHERE c.id = o.customer_id AND c.region = 'US'
);
```

### 7. Batch Aggregations

```sql
-- Multiple passes over data
SELECT COUNT(*) FROM sales;
SELECT SUM(amount) FROM sales;
SELECT AVG(amount) FROM sales;

-- Single pass
SELECT COUNT(*), SUM(amount), AVG(amount) FROM sales;
```

## Join Optimization

### Choose the Right Join Type

| Scenario | Recommended Join |
|----------|------------------|
| Small tables | Nested Loop |
| One small, one large | Hash Join (build on small) |
| Both large, equi-join | Hash Join |
| Non-equi conditions | Nested Loop |

### Join Order Matters

```sql
-- If orders is much larger than customers
SELECT * 
FROM orders o
JOIN customers c ON o.customer_id = c.id;

-- Optimizer should build hash table on smaller table (customers)
```

### Reduce Data Before Joining

```sql
-- Better performance
SELECT o.*, c.name
FROM (SELECT * FROM orders WHERE amount > 1000) o
JOIN customers c ON o.customer_id = c.id;
```

## Aggregation Optimization

### Pre-aggregate When Possible

```sql
-- If you often query by date
CREATE TABLE daily_sales AS
SELECT date, SUM(amount) as total
FROM sales
GROUP BY date;
```

### Use HAVING Sparingly

```sql
-- Filter before grouping when possible
SELECT dept_id, COUNT(*)
FROM employees
WHERE salary > 50000  -- Filter here
GROUP BY dept_id;

-- Only use HAVING for aggregate conditions
SELECT dept_id, COUNT(*)
FROM employees
GROUP BY dept_id
HAVING COUNT(*) > 10;  -- Must be HAVING
```

## Memory Usage

### Control Batch Sizes

For large results, process in batches:

```sql
-- Process 1000 rows at a time
SELECT * FROM large_table LIMIT 1000 OFFSET 0;
SELECT * FROM large_table LIMIT 1000 OFFSET 1000;
-- etc.
```

### Avoid Large IN Lists

```sql
-- Bad: Large memory allocation
SELECT * FROM products WHERE id IN (1, 2, 3, ... 10000);

-- Better: Use a temporary table or subquery
```

## Analyzing Query Performance

### Enable Timing

```sql
qe> .timing
Query timing enabled

qe> SELECT COUNT(*) FROM large_table;
-- Shows: Planning: 0.1ms, Execution: 45.2ms
```

### View Query Plan

```sql
qe> .plan

qe> SELECT * FROM employees WHERE dept_id = 5;
-- Shows:
-- Filter { predicate: dept_id = 5 }
--   └── TableScan { table: employees }
```

### Look For

- **Full table scans** on large tables (add index?)
- **Nested loops** on large tables (use hash join?)
- **Late filters** (push down?)
- **Unnecessary columns** (project earlier?)

## Checklist

Before running a query, ask:

- [ ] Are there indexes on filtered/joined columns?
- [ ] Am I selecting only needed columns?
- [ ] Is the JOIN order optimal?
- [ ] Can I add LIMIT to reduce results?
- [ ] Are there functions preventing index use?
- [ ] Is the filter pushed down as far as possible?

## Performance Tips Summary

| Issue | Solution |
|-------|----------|
| Slow lookups | Add index |
| Slow JOINs | Index join columns, optimize order |
| High memory | Reduce columns, add LIMIT |
| Complex subqueries | Rewrite as JOIN |
| Repeated aggregations | Pre-aggregate, cache |
