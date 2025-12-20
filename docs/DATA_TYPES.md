# Data Types Reference

Complete reference for data types supported by Query Engine.

## Numeric Types

### Integer Types

| Type | Size | Range | Use Case |
|------|------|-------|----------|
| `INT8` / `TINYINT` | 1 byte | -128 to 127 | Flags, small enums |
| `INT16` / `SMALLINT` | 2 bytes | -32,768 to 32,767 | Small integers |
| `INT32` / `INT` | 4 bytes | -2B to 2B | Standard integers |
| `INT64` / `BIGINT` | 8 bytes | ±9.2×10¹⁸ | Large integers, IDs |

### Unsigned Integers

| Type | Size | Range |
|------|------|-------|
| `UINT8` | 1 byte | 0 to 255 |
| `UINT16` | 2 bytes | 0 to 65,535 |
| `UINT32` | 4 bytes | 0 to 4B |
| `UINT64` | 8 bytes | 0 to 18×10¹⁸ |

### Floating Point

| Type | Size | Precision | Use Case |
|------|------|-----------|----------|
| `FLOAT32` / `REAL` | 4 bytes | ~7 digits | Scientific data |
| `FLOAT64` / `DOUBLE` | 8 bytes | ~15 digits | Financial, high precision |

### Decimal/Numeric

For exact decimal arithmetic:
```sql
DECIMAL(precision, scale)  -- e.g., DECIMAL(10, 2) for currency
NUMERIC(10, 2)             -- Same as DECIMAL
```

| Syntax | Example |
|--------|---------|
| `DECIMAL(p, s)` | `DECIMAL(10, 2)` for money |
| `NUMERIC` | Default precision (38, 9) |

## UUID Type

| Type | Size | Description |
|------|------|-------------|
| `UUID` | 16 bytes | Universally Unique Identifier |

```sql
CREATE TABLE users (
    id UUID NOT NULL,
    name VARCHAR
);
```

## JSON Type

| Type | Storage | Description |
|------|---------|-------------|
| `JSON` | Text | JSON document storage |
| `JSONB` | Text | Same as JSON |

```sql
CREATE TABLE events (
    id BIGINT,
    data JSON
);
```

## Array Types

Arrays of any base type using `[]` suffix:

| Syntax | Description |
|--------|-------------|
| `INT[]` | Array of integers |
| `TEXT[]` | Array of strings |
| `UUID[]` | Array of UUIDs |

```sql
CREATE TABLE products (
    id BIGINT,
    tags TEXT[],
    prices DECIMAL(10,2)[]
);
```

## Geometric Types

| Type | Format | Description |
|------|--------|-------------|
| `POINT` | `(x, y)` | 2D coordinate |
| `LINE` | `{A,B,C}` | Infinite line |
| `LSEG` | `[(x1,y1),(x2,y2)]` | Line segment |
| `BOX` | `((x1,y1),(x2,y2))` | Rectangle |
| `PATH` | `[(x1,y1),...]` | Connected points |
| `POLYGON` | `((x1,y1),...)` | Closed polygon |
| `CIRCLE` | `<(x,y),r>` | Circle |

```sql
CREATE TABLE locations (
    id BIGINT,
    coords POINT,
    area POLYGON,
    boundary BOX
);
```

## Interval Type

| Type | Storage | Description |
|------|---------|-------------|
| `INTERVAL` | MonthDayNano | Time duration |

```sql
CREATE TABLE tasks (
    id BIGINT,
    duration INTERVAL
);
```

## String Types

| Type | Description | Max Length |
|------|-------------|------------|
| `UTF8` / `VARCHAR` | Variable-length UTF-8 | ~2GB |
| `LARGESTRING` | Large strings | >2GB |

### Examples

```sql
CREATE TABLE users (
    name VARCHAR,          -- UTF8 string
    bio LARGESTRING        -- Large text field
);
```

## Boolean Type

| Type | Values |
|------|--------|
| `BOOLEAN` / `BOOL` | `TRUE`, `FALSE`, `NULL` |

### Boolean Operations

```sql
SELECT * FROM users WHERE active = TRUE;
SELECT * FROM products WHERE NOT discontinued;
SELECT * FROM orders WHERE paid AND shipped;
```

## Date & Time Types

| Type | Storage | Example |
|------|---------|---------|
| `DATE32` / `DATE` | Days since epoch | `'2024-01-15'` |
| `TIMESTAMP` | Microseconds | `'2024-01-15 10:30:00'` |
| `TIME` | Time of day | `'10:30:00'` |
| `INTERVAL` | Duration | `INTERVAL '1' DAY` |

### Date Literals

```sql
SELECT * FROM events WHERE date = '2024-01-15';
SELECT * FROM logs WHERE timestamp > '2024-01-01 00:00:00';
```

## Binary Types

| Type | Description |
|------|-------------|
| `BINARY` | Fixed-length bytes |
| `VARBINARY` | Variable-length bytes |

## Null Handling

All types can be nullable:

```sql
CREATE TABLE example (
    id INT64 NOT NULL,       -- Cannot be NULL
    name VARCHAR,            -- Can be NULL (default)
    email VARCHAR NOT NULL   -- Cannot be NULL
);
```

### NULL Operations

```sql
-- Check for NULL
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;

-- COALESCE returns first non-null
SELECT COALESCE(nickname, name, 'Anonymous') FROM users;

-- NULLIF returns NULL if values equal
SELECT NULLIF(divisor, 0) FROM data;  -- Avoids division by zero
```

## Type Conversions

### Implicit Conversions

| From | To | Allowed |
|------|----|---------|
| INT32 | INT64 | ✅ Auto |
| INT64 | FLOAT64 | ✅ Auto |
| INT → STRING | ❌ Explicit |

### Explicit Casting

```sql
SELECT CAST(amount AS INT64) FROM orders;
SELECT CAST(price AS VARCHAR) FROM products;
SELECT CAST('2024-01-15' AS DATE) FROM ...;
```

### Cast Functions

```sql
-- Numeric conversions
SELECT CAST(3.14 AS INT64);    -- 3
SELECT CAST('123' AS INT64);   -- 123

-- String conversions
SELECT CAST(42 AS VARCHAR);    -- '42'
SELECT CAST(TRUE AS VARCHAR);  -- 'true'

-- Date conversions
SELECT CAST('2024-01-15' AS DATE);
```

## Arrow Type Mapping

Query Engine uses Apache Arrow internally:

| SQL Type | Arrow Type |
|----------|------------|
| INT8 | Int8 |
| INT16 | Int16 |
| INT32 | Int32 |
| INT64 | Int64 |
| UINT8-64 | UInt8-64 |
| FLOAT32 | Float32 |
| FLOAT64 | Float64 |
| VARCHAR | Utf8 |
| BOOLEAN | Boolean |
| DATE | Date32 |
| TIMESTAMP | Timestamp |
| BINARY | Binary |

## Type Inference

When loading CSV files, types are inferred:

| Data Pattern | Inferred Type |
|--------------|---------------|
| `123`, `-45` | INT64 |
| `3.14`, `-0.5` | FLOAT64 |
| `true`, `false` | BOOLEAN |
| `2024-01-15` | DATE32 |
| Other | UTF8 |

### Override Inference

```sql
-- Specify schema when loading
.load csv data.csv mytable --schema "id:int64,name:string,active:bool"
```

## Comparison Rules

### Type Precedence

When comparing different types:

1. NULL < any value
2. Numeric comparisons use highest precision
3. String comparisons use lexicographic order

### String Comparison

```sql
-- Lexicographic (case-sensitive)
'apple' < 'banana'  -- TRUE
'Apple' < 'apple'   -- TRUE (uppercase < lowercase)
```

## Best Practices

1. **Use INT64 for IDs** - Avoids overflow issues
2. **Use FLOAT64 for money** - Better precision (or DECIMAL when available)
3. **Use timestamps** - For accurate time tracking
4. **Define NOT NULL** - When values are required
5. **Consider string length** - For validation and optimization

## Examples

### Table Definition

```sql
CREATE TABLE orders (
    id INT64 NOT NULL PRIMARY KEY,
    customer_id INT64 NOT NULL,
    amount FLOAT64 NOT NULL,
    currency VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    shipped_at TIMESTAMP,           -- Nullable
    notes VARCHAR                   -- Nullable
);
```

### Working with Types

```sql
-- Aggregate on numeric
SELECT SUM(amount), AVG(amount) FROM orders;

-- String operations
SELECT UPPER(name), LENGTH(description) FROM products;

-- Date arithmetic
SELECT date, date + 7 AS week_later FROM events;

-- Boolean logic
SELECT * FROM users WHERE active AND verified;
```
