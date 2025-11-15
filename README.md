# 🦆 DuckDB Data Viewer Pro

> **The most advanced data viewer for VS Code, powered by DuckDB WASM!**

Transform your VS Code into a powerful data analysis workbench. View, query, analyze, and transform data files with the full power of DuckDB - all without leaving your editor!

## ✨ Features at a Glance

### 📊 **Multi-Format Support**
Load and query files in various formats - all in the same viewer:
- CSV (.csv)
- Parquet (.parquet, .parq)
- Apache Arrow (.arrow, .ipc)
- JSON (.json)
- JSONL/NDJSON (.jsonl, .ndjson)
- SQLite (.sqlite) *(coming soon)*

### 🎯 **7 Powerful Tabs**

#### 1. **📊 Data View**
- Interactive sortable/filterable table
- Global search across all columns
- Per-column filtering
- SQL editor with syntax highlighting
- One-click export to CSV/JSON

#### 2. **📈 Data Profile**
- Automatic statistical analysis
- Column-level profiling (nulls, unique values, counts)
- Data quality insights
- One-click refresh

#### 3. **🔧 Query Builder**
- Visual drag-and-drop query construction
- No SQL knowledge required!
- Select columns, add filters, group by, order by
- Auto-generates SQL

#### 4. **🔄 Pivot Table**
- Interactive pivot table interface
- Drag columns to rows/columns/values
- Powered by DuckDB's native PIVOT
- Perfect for cross-tabulations

#### 5. **📉 Charts**
- Visualize query results
- Bar, Line, Scatter, Pie, Histogram charts
- Configure X/Y axes
- Export visualizations

#### 6. **🕐 Query History**
- Automatic query logging
- Execution time tracking
- Row count for each query
- One-click rerun

#### 7. **📚 Query Templates**
- 10+ pre-built query templates
- Save your own templates
- Auto-substitutes table/column names
- Great for learning SQL!

### 🚀 Advanced Features

- **Multi-File Operations**: Load multiple files and JOIN them together
- **Full DuckDB SQL**: Window functions, CTEs, aggregates, and more
- **Export Anywhere**: Export query results to CSV, JSON
- **Fast Performance**: Vectorized execution, columnar storage
- **Zero Installation**: Runs entirely in the browser via WASM

## 📦 Installation

1. Install from VS Code Marketplace (or build from source)
2. Right-click any supported file in VS Code Explorer
3. Select "View File with DuckDB"
4. Start analyzing! 🎉

## 🎓 Quick Start

### View a CSV File
1. Right-click a `.csv` file → "View File with DuckDB"
2. Data loads automatically with default query
3. Use the table filters or write custom SQL

### Run a Query
1. Type your SQL in the SQL Editor (bottom panel)
2. Press `Cmd/Ctrl + Enter` or click "Run Query"
3. Results appear in the table above

### Profile Your Data
1. Click the **📈 Data Profile** tab
2. See automatic statistics for all columns
3. Click "Refresh Profile" after running queries

### Build a Query Visually
1. Click the **🔧 Query Builder** tab
2. Click columns to select them
3. Add filters, grouping, ordering
4. Click "Generate SQL"
5. Query appears in the SQL Editor

### Create a Pivot Table
1. Click the **🔄 Pivot Table** tab
2. Click columns to add them to Rows/Columns/Values
3. Click "Generate Pivot"
4. SQL is generated and executed

### Use Templates
1. Click the **📚 Templates** tab
2. Click any template (e.g., "Top N Rows")
3. Template SQL appears in editor with your table name
4. Modify and run!

## 🔥 Example Queries

### Basic Selection
```sql
SELECT * FROM my_data LIMIT 100;
```

### Aggregation
```sql
SELECT
  category,
  COUNT(*) as count,
  AVG(price) as avg_price,
  MAX(price) as max_price
FROM my_data
GROUP BY category
ORDER BY count DESC;
```

### Window Functions
```sql
SELECT
  name,
  department,
  salary,
  RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
  AVG(salary) OVER (PARTITION BY department) as dept_avg_salary
FROM employees;
```

### Find Duplicates
```sql
SELECT *, COUNT(*) as occurrences
FROM my_data
GROUP BY ALL
HAVING COUNT(*) > 1;
```

### Date Filtering
```sql
SELECT *
FROM orders
WHERE order_date >= DATE '2024-01-01'
  AND order_date < DATE '2024-02-01';
```

### JOIN Multiple Files
Load two CSV files, then:
```sql
SELECT
  customers.name,
  customers.email,
  orders.order_total,
  orders.order_date
FROM customers_csv_1234 customers
JOIN orders_csv_5678 orders
  ON customers.customer_id = orders.customer_id
WHERE orders.order_total > 100
ORDER BY orders.order_date DESC;
```

## 💡 DuckDB Superpowers

This extension gives you access to DuckDB's incredible features:

### Advanced Analytics
- **Window Functions**: `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`, `NTILE()`
- **Aggregates**: `COUNT()`, `SUM()`, `AVG()`, `MEDIAN()`, `MODE()`, `PERCENTILE_CONT()`
- **Statistical Functions**: `STDDEV()`, `VARIANCE()`, `CORR()`, `REGR_SLOPE()`

### String Operations
- **Pattern Matching**: `LIKE`, `REGEXP_MATCHES()`, `REGEXP_REPLACE()`
- **String Functions**: `SPLIT()`, `CONCAT()`, `SUBSTRING()`, `TRIM()`, `UPPER()`, `LOWER()`
- **Fuzzy Matching**: `LEVENSHTEIN()`, `JARO_WINKLER_SIMILARITY()`

### Date/Time
- **Date Arithmetic**: `DATE '2024-01-01' + INTERVAL 7 DAY`
- **Date Functions**: `DATE_TRUNC()`, `DATE_DIFF()`, `EXTRACT()`
- **Sequences**: `GENERATE_SERIES(start, end, step)`

### Array & Struct
- **List Functions**: `LIST_AGGREGATE()`, `LIST_FILTER()`, `UNNEST()`
- **Struct Functions**: `STRUCT_EXTRACT()`, `STRUCT_PACK()`
- **JSON Functions**: `JSON_EXTRACT()`, `TO_JSON()`, `JSON_TRANSFORM()`

### DuckDB-Specific Features
- **GROUP BY ALL**: Automatically groups by non-aggregated columns
- **QUALIFY**: Filter window function results
- **PIVOT/UNPIVOT**: Reshape data easily
- **SAMPLE**: Random sampling for large datasets
- **ASOF JOIN**: Time-series joins

## 📚 Built-in Query Templates

1. **Top N Rows** - Quick data preview
2. **Group By Count** - Count occurrences
3. **Find Duplicates** - Data quality check
4. **Column Statistics** - Numeric summaries
5. **Null Analysis** - Find missing data
6. **Date Range Filter** - Time-based filtering
7. **Top Values** - Most common values
8. **Window Function - Rank** - Ranking analysis
9. **Running Total** - Cumulative calculations
10. **Cross Tab / Pivot** - Data reshaping

## 🎨 Use Cases

- **Data Exploration**: Quickly understand new datasets
- **Data Cleaning**: Find nulls, duplicates, outliers
- **Data Analysis**: Aggregate, pivot, visualize
- **Data Transformation**: Transform and export
- **Data QA**: Validate data quality
- **Learning SQL**: Use templates and query builder
- **Ad-hoc Queries**: Quick analysis without database setup

## 🛠️ Technical Details

- **Engine**: DuckDB WASM 1.30.0
- **Format**: Apache Arrow for data interchange
- **Storage**: In-memory columnar database
- **Performance**: Vectorized SIMD execution
- **Size**: ~10MB (includes full DuckDB engine)

## 📖 Documentation

For detailed documentation of all features, see [FEATURES.md](FEATURES.md).

## 🚀 Building from Source

```bash
npm install
npm run compile
# Press F5 in VS Code to launch extension development host
```

## 🤝 Contributing

Contributions welcome! This extension is open source.

## 📄 License

MIT License

## 🙏 Credits

Built with:
- [DuckDB](https://duckdb.org/) - Amazing analytical database
- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
- [VS Code Extension API](https://code.visualstudio.com/api) - Extension framework

---

**Happy Data Exploring! 🦆✨**
