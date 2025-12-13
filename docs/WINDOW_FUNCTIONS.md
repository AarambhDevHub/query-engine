# Window Functions Guide

Complete guide to window functions in Query Engine.

## Overview

Window functions compute values across a set of rows related to the current row, without collapsing them into a single output row like aggregate functions do.

## Syntax

```sql
function_name(args) OVER (
    [PARTITION BY partition_expression, ...]
    [ORDER BY sort_expression [ASC|DESC], ...]
    [frame_clause]
)
```

## Basic Example

```sql
SELECT 
    name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg
FROM employees;
```

Result:
```
┌───────────┬────────────┬────────┬──────────┐
│ name      │ department │ salary │ dept_avg │
├───────────┼────────────┼────────┼──────────┤
│ Alice     │ Sales      │ 60000  │ 65000    │
│ Bob       │ Sales      │ 70000  │ 65000    │
│ Charlie   │ Eng        │ 80000  │ 85000    │
│ Diana     │ Eng        │ 90000  │ 85000    │
└───────────┴────────────┴────────┴──────────┘
```

## Ranking Functions

### ROW_NUMBER()

Assigns sequential numbers to rows:

```sql
SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank
FROM employees;
```

```
┌───────────┬────────┬──────┐
│ name      │ salary │ rank │
├───────────┼────────┼──────┤
│ Diana     │ 90000  │ 1    │
│ Charlie   │ 80000  │ 2    │
│ Bob       │ 70000  │ 3    │
│ Alice     │ 60000  │ 4    │
└───────────┴────────┴──────┘
```

### RANK()

Assigns ranks with gaps for ties:

```sql
SELECT 
    name,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;
```

With ties:
```
┌───────────┬────────┬──────┐
│ name      │ salary │ rank │
├───────────┼────────┼──────┤
│ Diana     │ 90000  │ 1    │
│ Charlie   │ 80000  │ 2    │
│ Eve       │ 80000  │ 2    │  ← Same rank
│ Alice     │ 60000  │ 4    │  ← Skips 3
└───────────┴────────┴──────┘
```

### DENSE_RANK()

Ranks without gaps:

```sql
SELECT 
    name,
    salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;
```

```
┌───────────┬────────┬──────┐
│ name      │ salary │ rank │
├───────────┼────────┼──────┤
│ Diana     │ 90000  │ 1    │
│ Charlie   │ 80000  │ 2    │
│ Eve       │ 80000  │ 2    │  ← Same rank
│ Alice     │ 60000  │ 3    │  ← No gap
└───────────┴────────┴──────┘
```

### NTILE(n)

Divides rows into n groups:

```sql
SELECT 
    name,
    salary,
    NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM employees;
```

```
┌───────────┬────────┬──────────┐
│ name      │ salary │ quartile │
├───────────┼────────┼──────────┤
│ Diana     │ 90000  │ 1        │
│ Charlie   │ 80000  │ 2        │
│ Bob       │ 70000  │ 3        │
│ Alice     │ 60000  │ 4        │
└───────────┴────────┴──────────┘
```

## Value Functions

### LAG(column, n)

Access value from n rows before:

```sql
SELECT 
    date,
    sales,
    LAG(sales, 1) OVER (ORDER BY date) AS prev_sales,
    sales - LAG(sales, 1) OVER (ORDER BY date) AS change
FROM daily_sales;
```

```
┌────────────┬───────┬────────────┬────────┐
│ date       │ sales │ prev_sales │ change │
├────────────┼───────┼────────────┼────────┤
│ 2024-01-01 │ 100   │ NULL       │ NULL   │
│ 2024-01-02 │ 120   │ 100        │ 20     │
│ 2024-01-03 │ 115   │ 120        │ -5     │
└────────────┴───────┴────────────┴────────┘
```

### LEAD(column, n)

Access value from n rows after:

```sql
SELECT 
    date,
    sales,
    LEAD(sales, 1) OVER (ORDER BY date) AS next_sales
FROM daily_sales;
```

### FIRST_VALUE(column)

First value in the window:

```sql
SELECT 
    name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary DESC
    ) AS top_salary
FROM employees;
```

### LAST_VALUE(column)

Last value in the window:

```sql
SELECT 
    name,
    department,
    salary,
    LAST_VALUE(salary) OVER (
        PARTITION BY department 
        ORDER BY salary
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS max_salary
FROM employees;
```

## Aggregate Window Functions

Any aggregate can be used as a window function:

```sql
SELECT 
    name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) AS dept_total,
    COUNT(*) OVER (PARTITION BY department) AS dept_count,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg,
    MIN(salary) OVER (PARTITION BY department) AS dept_min,
    MAX(salary) OVER (PARTITION BY department) AS dept_max
FROM employees;
```

## Frame Clauses

Control which rows are included in the window.

### Syntax

```sql
ROWS BETWEEN start AND end
RANGE BETWEEN start AND end
```

### Boundaries

| Boundary | Description |
|----------|-------------|
| `UNBOUNDED PRECEDING` | First row of partition |
| `n PRECEDING` | n rows before current |
| `CURRENT ROW` | Current row |
| `n FOLLOWING` | n rows after current |
| `UNBOUNDED FOLLOWING` | Last row of partition |

### Examples

**Running total:**
```sql
SELECT 
    date,
    amount,
    SUM(amount) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM transactions;
```

**Moving average:**
```sql
SELECT 
    date,
    value,
    AVG(value) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3
FROM metrics;
```

**Previous and next:**
```sql
SELECT 
    position,
    LAG(value) OVER (ORDER BY position) AS prev,
    value AS current,
    LEAD(value) OVER (ORDER BY position) AS next
FROM data;
```

## PARTITION BY

Divides rows into groups for separate calculations:

```sql
-- Rank within each department
SELECT 
    name,
    department,
    salary,
    RANK() OVER (
        PARTITION BY department 
        ORDER BY salary DESC
    ) AS dept_rank
FROM employees;
```

```
┌───────────┬────────────┬────────┬───────────┐
│ name      │ department │ salary │ dept_rank │
├───────────┼────────────┼────────┼───────────┤
│ Diana     │ Eng        │ 90000  │ 1         │
│ Charlie   │ Eng        │ 80000  │ 2         │
│ Bob       │ Sales      │ 70000  │ 1         │
│ Alice     │ Sales      │ 60000  │ 2         │
└───────────┴────────────┴────────┴───────────┘
```

## Use Cases

### Top N per Group

```sql
-- Top 3 earners in each department
SELECT * FROM (
    SELECT 
        name,
        department,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department 
            ORDER BY salary DESC
        ) AS rn
    FROM employees
) WHERE rn <= 3;
```

### Percent of Total

```sql
SELECT 
    department,
    SUM(amount) AS dept_total,
    SUM(amount) * 100.0 / SUM(SUM(amount)) OVER () AS pct_of_total
FROM sales
GROUP BY department;
```

### Year-over-Year Comparison

```sql
SELECT 
    year,
    quarter,
    revenue,
    LAG(revenue, 4) OVER (ORDER BY year, quarter) AS prev_year,
    revenue - LAG(revenue, 4) OVER (ORDER BY year, quarter) AS yoy_change
FROM quarterly_revenue;
```

### Cumulative Distribution

```sql
SELECT 
    name,
    salary,
    CUME_DIST() OVER (ORDER BY salary) AS percentile
FROM employees;
```

## Summary Table

| Function | Purpose |
|----------|---------|
| `ROW_NUMBER()` | Unique sequential number |
| `RANK()` | Rank with gaps |
| `DENSE_RANK()` | Rank without gaps |
| `NTILE(n)` | Divide into n groups |
| `LAG(col, n)` | Value n rows before |
| `LEAD(col, n)` | Value n rows after |
| `FIRST_VALUE(col)` | First value in window |
| `LAST_VALUE(col)` | Last value in window |
| `SUM() OVER` | Running/partitioned sum |
| `AVG() OVER` | Running/partitioned average |
