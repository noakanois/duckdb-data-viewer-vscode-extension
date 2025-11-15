# DuckDB Data Viewer Pro - Amazing Features! 🚀

This VS Code extension supercharges your data analysis workflow with the power of DuckDB WASM! Here are all the incredible features:

## 🎯 Core Features

### 📊 **Data View Tab**
The main data viewing interface with powerful capabilities:

- **Interactive Data Table**
  - Sortable columns (click headers to cycle: none → ascending → descending)
  - Per-column filters (type in the filter row below headers)
  - Global search across all columns
  - Sticky headers for easy navigation
  - Smart row counting (shows filtered vs total)

- **SQL Editor**
  - Full-featured SQL query editor
  - Syntax highlighting (monospace font)
  - Keyboard shortcut: `Cmd/Ctrl + Enter` to run queries
  - Copy SQL to clipboard
  - Auto-generated default queries

- **Export Data** - One-click export to multiple formats:
  - 📄 Export to CSV
  - 📦 Export to JSON
  - 🗄️ Export to Parquet (coming soon)

### 📈 **Data Profile Tab**
Automatic statistical analysis of your data:

- **Overview Statistics**
  - Total row count
  - Total column count

- **Column-Level Profiling**
  - Unique value count for each column
  - Non-null value count
  - Null count with percentage
  - Data type information
  - (Future: min/max, distributions, outlier detection)

### 🔧 **Query Builder Tab**
Visual query builder - no SQL knowledge required!

- **Drag-and-Drop Interface**
  - Select columns to include
  - Add filter conditions
  - Choose GROUP BY columns
  - Set ORDER BY columns
  - Specify LIMIT

- **Generate SQL** button creates the query for you
- Perfect for SQL beginners or quick exploration

### 🔄 **Pivot Table Tab**
Interactive pivot table powered by DuckDB's PIVOT function:

- **Drag columns to:**
  - Rows (dimension to group by rows)
  - Columns (dimension to pivot to columns)
  - Values (metrics to aggregate)

- **One-click pivot generation**
- Creates complex PIVOT queries automatically
- Great for creating cross-tabulations and summaries

### 📉 **Charts Tab**
Data visualization from query results:

- **Chart Types**
  - Bar Chart
  - Line Chart
  - Scatter Plot
  - Pie Chart
  - Histogram

- **Configuration**
  - Choose X-axis column
  - Choose Y-axis column
  - Limit number of rows to visualize

- Note: Currently shows data table preview. Full chart rendering library integration coming soon!

### 🕐 **History Tab**
Query history tracking:

- **Automatic logging** of all executed queries
- **Metadata tracked:**
  - Query text
  - Timestamp
  - Rows returned
  - Execution time

- **Click any query** to rerun it
- **Clear history** button
- Keeps last 50 queries

### 📚 **Templates Tab**
Pre-built query templates for common tasks:

#### Built-in Templates:

1. **Top N Rows** - Get the first N rows from your data
2. **Group By Count** - Count occurrences by a column
3. **Find Duplicates** - Find duplicate rows based on all columns
4. **Column Statistics** - Get statistical summary of numeric columns
5. **Null Analysis** - Find rows with null values
6. **Date Range Filter** - Filter data by date range
7. **Top Values** - Find most common values in a column
8. **Window Function - Rank** - Rank rows by a numeric column
9. **Running Total** - Calculate cumulative sum
10. **Cross Tab / Pivot** - Create a pivot table

**Save your own templates:**
- Click "Save Current Query" to save any query as a reusable template
- Add name and description
- Templates auto-substitute table and column names

## 🔥 Advanced DuckDB Features

### Multi-Format Support
Load and query files in various formats:

- **CSV** (.csv) - Comma-separated values
- **Parquet** (.parquet, .parq) - Columnar storage format
- **Arrow** (.arrow, .ipc) - Apache Arrow IPC format
- **JSON** (.json) - JSON documents
- **JSONL/NDJSON** (.jsonl, .ndjson) - Newline-delimited JSON
- **SQLite** (.sqlite) - SQLite databases (via DuckDB attach)

### 🤝 Multi-File Operations
Load multiple files simultaneously:

- **File Manager** shows all loaded files
- **File chips** display:
  - File name
  - Row count
  - Remove button (×)

- **JOIN files together** using SQL:
  ```sql
  SELECT a.*, b.column
  FROM file1_data a
  JOIN file2_data b ON a.id = b.id
  ```

### ⚡ DuckDB SQL Superpowers

Take advantage of DuckDB's advanced SQL features:

#### Window Functions
```sql
SELECT
  name,
  salary,
  RANK() OVER (ORDER BY salary DESC) as rank,
  AVG(salary) OVER () as avg_salary
FROM employees
```

#### Common Table Expressions (CTEs)
```sql
WITH summary AS (
  SELECT category, COUNT(*) as count
  FROM products
  GROUP BY category
)
SELECT * FROM summary WHERE count > 10
```

#### Array and Struct Functions
```sql
SELECT
  list_aggregate([1, 2, 3, 4], 'sum'),
  struct_extract({'a': 1, 'b': 2}, 'a')
```

#### String Functions
```sql
SELECT
  regexp_replace(text, '[0-9]+', 'X'),
  string_split(text, ','),
  levenshtein('hello', 'hallo')
FROM data
```

#### Date/Time Functions
```sql
SELECT
  date_trunc('month', timestamp_col),
  date_diff('day', start_date, end_date),
  generate_series(DATE '2024-01-01', DATE '2024-12-31', INTERVAL 1 DAY)
FROM events
```

#### Aggregate Functions
```sql
SELECT
  category,
  COUNT(*) as count,
  AVG(price) as avg_price,
  MEDIAN(price) as median_price,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) as p95_price,
  MODE(color) as most_common_color
FROM products
GROUP BY category
```

## 🎨 Use Cases

### 1. **Data Exploration**
- Load a CSV/Parquet file
- Use Data Profile tab to understand the data
- Use Query Builder to slice and dice without writing SQL

### 2. **Data Cleaning**
- Use Templates → "Find Duplicates" to find duplicate records
- Use Templates → "Null Analysis" to find missing data
- Write custom queries to clean and transform

### 3. **Data Analysis**
- Use Pivot Table to create cross-tabulations
- Use Charts to visualize trends
- Use Window Functions for advanced analytics

### 4. **Data Transformation**
- Load source data
- Write transformation SQL
- Export to CSV/JSON for downstream use

### 5. **Quick Data QA**
- Check row counts
- Validate data types
- Find outliers and anomalies
- Compare datasets with JOINs

## 🌟 Power User Tips

### Tip 1: Query Multiple Files
```sql
-- Load 2 CSV files, then JOIN them:
SELECT
  customers.name,
  orders.total_amount
FROM customers_csv
JOIN orders_csv ON customers.id = orders.customer_id
```

### Tip 2: Use DESCRIBE to Understand Schema
```sql
DESCRIBE my_data;
-- Shows column names, types, and constraints
```

### Tip 3: Use SUMMARIZE for Quick Stats
```sql
SUMMARIZE my_data;
-- Auto-generates statistics for all columns
```

### Tip 4: Create Derived Columns
```sql
SELECT
  *,
  price * quantity as total,
  CASE WHEN quantity > 10 THEN 'bulk' ELSE 'retail' END as order_type
FROM orders
```

### Tip 5: Filter with Complex Conditions
```sql
SELECT * FROM data
WHERE
  date >= '2024-01-01'
  AND category IN ('A', 'B', 'C')
  AND price BETWEEN 10 AND 100
  AND description LIKE '%important%'
```

### Tip 6: Use GROUP BY ALL
```sql
-- DuckDB's GROUP BY ALL automatically groups by all non-aggregated columns
SELECT category, brand, COUNT(*), AVG(price)
FROM products
GROUP BY ALL
```

### Tip 7: Save Complex Queries as Templates
- Write your query
- Click "Save Current Query" in Templates tab
- Reuse across different datasets

## 🔮 Coming Soon

### Features in Development:

1. **HTTP/S3 Remote File Loading**
   - Load files directly from URLs
   - Query S3 buckets
   - Access cloud data without downloading

2. **Advanced Chart Library Integration**
   - Full D3.js or Vega-Lite charts
   - Interactive visualizations
   - Export charts as images

3. **SQL Formatter**
   - Auto-format SQL queries
   - Syntax validation
   - Query optimization hints

4. **Relationship Auto-Detection**
   - Automatically detect foreign key relationships
   - Suggest JOINs
   - Visual relationship diagrams

5. **Data Diff Tool**
   - Compare two query results
   - Highlight differences
   - Track changes over time

6. **Export to More Formats**
   - Excel (.xlsx)
   - SQLite database
   - Apache Parquet (browser support)

7. **Collaborative Features**
   - Share queries with team
   - Query library
   - Comments and annotations

8. **Performance Insights**
   - Query EXPLAIN plans
   - Optimization suggestions
   - Performance metrics

## 🛠️ Technical Details

### DuckDB Extensions Loaded:
- **Parquet** - Read/write Parquet files
- **SQLite** - Attach SQLite databases
- **JSON** - Read JSON and JSONL files

### Architecture:
- **DuckDB WASM** - Full DuckDB engine in the browser
- **Apache Arrow** - High-performance data interchange
- **In-Memory Database** - Fast query execution
- **Persistent WebView** - State maintained across file loads

### Performance:
- **Columnar Storage** - Optimized for analytical queries
- **Vectorized Execution** - SIMD-optimized query processing
- **Zero-Copy** - Efficient data transfer
- **Lazy Loading** - Only loads data when needed

## 💡 Pro Tips for DuckDB

### Use COPY for Large Exports
```sql
COPY (SELECT * FROM huge_table)
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

### Use SAMPLE for Quick Previews
```sql
SELECT * FROM large_table USING SAMPLE 1%;
-- Randomly sample 1% of rows
```

### Use QUALIFY for Window Function Filtering
```sql
SELECT
  name,
  salary,
  RANK() OVER (ORDER BY salary DESC) as rank
FROM employees
QUALIFY rank <= 10
-- Only show top 10 salaries
```

### Use PIVOT for Reshaping Data
```sql
PIVOT products
ON category
USING SUM(sales)
GROUP BY region;
```

### Use UNPIVOT for Melting Data
```sql
UNPIVOT sales_by_quarter
ON Q1, Q2, Q3, Q4
INTO NAME quarter VALUE amount;
```

## 📖 Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Apache Arrow](https://arrow.apache.org/)

## 🎉 Enjoy!

This extension brings the full power of DuckDB to your VS Code editor. Happy querying! 🦆✨
