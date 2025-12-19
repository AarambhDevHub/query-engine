# SQL Reference

Complete reference for SQL syntax supported by Query Engine.

## Statements

### SELECT

```sql
SELECT [DISTINCT] 
    expression [AS alias], ...
FROM table_name [alias]
[JOIN join_table ON condition]
[WHERE condition]
[GROUP BY expression, ...]
[HAVING condition]
[ORDER BY expression [ASC|DESC], ...]
[LIMIT count]
[OFFSET start]
```

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column_name data_type [NOT NULL] [PRIMARY KEY],
    ...
);
```

### CREATE INDEX

```sql
CREATE [UNIQUE] INDEX index_name 
ON table_name (column_name, ...)
[USING {BTREE | HASH}];
```

### DROP INDEX

```sql
DROP INDEX [IF EXISTS] index_name [ON table_name];
```

### INSERT

```sql
INSERT INTO table_name (column, ...)
VALUES (value, ...), ...;
```

### UPDATE

```sql
UPDATE table_name
SET column = value, ...
[WHERE condition];
```

### DELETE

```sql
DELETE FROM table_name
[WHERE condition];
```

---

## Data Types

| Type | Description | Example |
|------|-------------|---------|
| `INT8` | 8-bit signed integer | `-128` to `127` |
| `INT16` | 16-bit signed integer | `-32768` to `32767` |
| `INT32` | 32-bit signed integer | Standard integer |
| `INT64` | 64-bit signed integer | `BIGINT` |
| `UINT8`-`UINT64` | Unsigned integers | Positive only |
| `FLOAT32` | 32-bit floating point | `REAL` |
| `FLOAT64` | 64-bit floating point | `DOUBLE` |
| `UTF8` | Variable-length string | `VARCHAR` |
| `BOOLEAN` | True/False | `TRUE`, `FALSE` |
| `DATE32` | Date (32-bit) | `'2024-01-15'` |
| `TIMESTAMP` | Date and time | `'2024-01-15 10:30:00'` |

---

## Operators

### Comparison

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=`, `<>` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |
| `IS NULL` | Is null |
| `IS NOT NULL` | Is not null |
| `BETWEEN...AND` | Range inclusive |
| `IN (...)` | In list |
| `LIKE` | Pattern matching |

### Logical

| Operator | Description |
|----------|-------------|
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical NOT |

### Arithmetic

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

---

## Functions

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count distinct values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(s)` | Uppercase | `UPPER('hello')` → `'HELLO'` |
| `LOWER(s)` | Lowercase | `LOWER('HELLO')` → `'hello'` |
| `LENGTH(s)` | String length | `LENGTH('hello')` → `5` |
| `CONCAT(a, b, ...)` | Concatenate | `CONCAT('a', 'b')` → `'ab'` |
| `SUBSTRING(s, start, len)` | Extract substring | `SUBSTRING('hello', 2, 3)` → `'ell'` |
| `TRIM(s)` | Remove whitespace | `TRIM('  hi  ')` → `'hi'` |
| `REPLACE(s, from, to)` | Replace substring | `REPLACE('abc', 'b', 'x')` → `'axc'` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(x)` | Absolute value | `ABS(-5)` → `5` |
| `CEIL(x)` | Round up | `CEIL(4.2)` → `5` |
| `FLOOR(x)` | Round down | `FLOOR(4.8)` → `4` |
| `ROUND(x, d)` | Round to decimals | `ROUND(3.14159, 2)` → `3.14` |
| `SQRT(x)` | Square root | `SQRT(16)` → `4` |
| `POWER(x, y)` | Exponentiation | `POWER(2, 3)` → `8` |

### Null Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COALESCE(a, b, ...)` | First non-null | `COALESCE(NULL, 5)` → `5` |
| `NULLIF(a, b)` | NULL if equal | `NULLIF(5, 5)` → `NULL` |

### Window Functions

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Sequential row number |
| `RANK()` | Rank with gaps |
| `DENSE_RANK()` | Rank without gaps |
| `NTILE(n)` | Divide into n groups |
| `LAG(col, n)` | Value n rows before |
| `LEAD(col, n)` | Value n rows after |
| `FIRST_VALUE(col)` | First value in window |
| `LAST_VALUE(col)` | Last value in window |

---

## JOIN Types

### INNER JOIN

Returns rows with matches in both tables.

```sql
SELECT * FROM a INNER JOIN b ON a.id = b.a_id;
```

### LEFT JOIN

Returns all rows from left table, matched rows from right.

```sql
SELECT * FROM a LEFT JOIN b ON a.id = b.a_id;
```

### RIGHT JOIN

Returns all rows from right table, matched rows from left.

```sql
SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id;
```

### FULL OUTER JOIN

Returns all rows from both tables.

```sql
SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id;
```

### CROSS JOIN

Returns Cartesian product (all combinations).

```sql
SELECT * FROM a CROSS JOIN b;
```

---

## Subqueries

### Scalar Subquery

```sql
SELECT name, (SELECT AVG(salary) FROM employees) as avg_salary
FROM employees;
```

### IN Subquery

```sql
SELECT * FROM employees
WHERE dept_id IN (SELECT id FROM departments WHERE location = 'NYC');
```

### EXISTS Subquery

```sql
SELECT * FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);
```

### Derived Table

```sql
SELECT sub.name, sub.total
FROM (SELECT name, SUM(amount) as total FROM sales GROUP BY name) AS sub
WHERE sub.total > 1000;
```

---

## Common Table Expressions (CTEs)

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

### Multiple CTEs

```sql
WITH 
    cte1 AS (SELECT ...),
    cte2 AS (SELECT ... FROM cte1)
SELECT * FROM cte2;
```

---

## Window Functions

### Syntax

```sql
function_name() OVER (
    [PARTITION BY column, ...]
    [ORDER BY column [ASC|DESC], ...]
    [frame_clause]
)
```

### Frame Clause

```sql
ROWS BETWEEN start AND end
RANGE BETWEEN start AND end
```

Where start/end can be:
- `UNBOUNDED PRECEDING`
- `n PRECEDING`
- `CURRENT ROW`
- `n FOLLOWING`
- `UNBOUNDED FOLLOWING`

### Example

```sql
SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM employees;
```

---

## Index Usage

### Creating Indexes

```sql
-- B-Tree index (default)
CREATE INDEX idx_name ON employees(name);

-- Hash index
CREATE INDEX idx_email ON users(email) USING HASH;

-- Unique index
CREATE UNIQUE INDEX idx_id ON employees(id);

-- Multi-column index
CREATE INDEX idx_composite ON orders(customer_id, order_date);
```

### When Indexes Help

- `WHERE column = value` (equality)
- `WHERE column BETWEEN a AND b` (range, B-Tree only)
- `ORDER BY column` (B-Tree only)
- `JOIN ... ON column` 

### Dropping Indexes

```sql
DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_name;
```
