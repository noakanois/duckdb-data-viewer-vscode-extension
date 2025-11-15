import * as duckdb from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow';
import { csvLoader } from './loaders/csvLoader';
import { arrowLoader } from './loaders/arrowLoader';
import { parquetLoader } from './loaders/parquetLoader';
import { jsonLoader } from './loaders/jsonLoader';
import { DataLoader } from './loaders/types';
import { buildDefaultQuery } from './utils/sqlHelpers';

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

// Get UI elements
const status = document.getElementById('status');
const controls = document.getElementById('controls');
const resultsContainer = document.getElementById('results-container');
const sqlInput = document.getElementById('sql-input') as HTMLTextAreaElement;
const runButton = document.getElementById('run-query') as HTMLButtonElement;
const copySqlButton = document.getElementById('copy-sql') as HTMLButtonElement;
const statusWrapper = document.getElementById('status-wrapper');
const globalSearchInput = document.getElementById('global-search') as HTMLInputElement;
const rowCountLabel = document.getElementById('row-count');
const fileManager = document.getElementById('file-manager');
const fileList = document.getElementById('file-list');

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface LoadedFile {
  name: string;
  relationName: string;
  columns: string[];
  rowCount: number;
}

interface QueryHistoryItem {
  query: string;
  timestamp: number;
  rowsReturned: number;
  executionTime: number;
}

interface QueryTemplate {
  name: string;
  description: string;
  sql: string;
}

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let duckdbInitializationPromise: Promise<void> | null = null;
let currentTableData: TableData | null = null;
let columnFilters: string[] = [];
let globalFilter = '';
let sortState: { columnIndex: number; direction: SortDirection } = { columnIndex: -1, direction: null };
let tableBodyElement: HTMLTableSectionElement | null = null;
let copyTimeoutHandle: number | null = null;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, jsonLoader, csvLoader];
const loadedFiles: LoadedFile[] = [];
const queryHistory: QueryHistoryItem[] = [];
const queryTemplates: QueryTemplate[] = [
  {
    name: 'Top N Rows',
    description: 'Get the first N rows from your data',
    sql: 'SELECT * FROM {table} LIMIT 10'
  },
  {
    name: 'Group By Count',
    description: 'Count occurrences by a column',
    sql: 'SELECT {column}, COUNT(*) as count FROM {table} GROUP BY {column} ORDER BY count DESC'
  },
  {
    name: 'Find Duplicates',
    description: 'Find duplicate rows based on all columns',
    sql: 'SELECT *, COUNT(*) as occurrences FROM {table} GROUP BY ALL HAVING COUNT(*) > 1'
  },
  {
    name: 'Column Statistics',
    description: 'Get statistical summary of numeric columns',
    sql: 'SELECT COUNT(*) as total_rows, COUNT(DISTINCT {column}) as unique_values, MIN({column}) as min, MAX({column}) as max, AVG({column}) as avg FROM {table}'
  },
  {
    name: 'Null Analysis',
    description: 'Find rows with null values',
    sql: 'SELECT * FROM {table} WHERE {column} IS NULL'
  },
  {
    name: 'Date Range Filter',
    description: 'Filter data by date range',
    sql: 'SELECT * FROM {table} WHERE {date_column} BETWEEN \'2024-01-01\' AND \'2024-12-31\''
  },
  {
    name: 'Top Values',
    description: 'Find most common values in a column',
    sql: 'SELECT {column}, COUNT(*) as frequency FROM {table} GROUP BY {column} ORDER BY frequency DESC LIMIT 20'
  },
  {
    name: 'Window Function - Rank',
    description: 'Rank rows by a numeric column',
    sql: 'SELECT *, RANK() OVER (ORDER BY {column} DESC) as rank FROM {table}'
  },
  {
    name: 'Running Total',
    description: 'Calculate cumulative sum',
    sql: 'SELECT *, SUM({column}) OVER (ORDER BY {order_column}) as running_total FROM {table}'
  },
  {
    name: 'Cross Tab / Pivot',
    description: 'Create a pivot table',
    sql: 'PIVOT {table} ON {column} USING SUM({value_column})'
  },
];

let currentActiveTab = 'data';

// --- Event Listeners ---

window.addEventListener('message', (event: any) => {
  const message = event.data;
  if (message.command === 'init') {
    ensureDuckDBInitialized(message.bundles).catch(reportError);
  } else if (message.command === 'loadFile') {
    handleFileLoad(message.fileName, message.fileData).catch(reportError);
  } else if (message.command === 'error') {
    reportError(message.message);
  }
});

runButton.addEventListener('click', () => {
  runQuery(sqlInput.value).catch(reportError);
});

sqlInput.addEventListener('keydown', (event: KeyboardEvent) => {
  const isSubmitShortcut = event.key === 'Enter' && (event.metaKey || event.ctrlKey);
  if (isSubmitShortcut) {
    event.preventDefault();
    runQuery(sqlInput.value).catch(reportError);
  }
});

if (globalSearchInput) {
  globalSearchInput.addEventListener('input', () => {
    globalFilter = globalSearchInput.value;
    applyTableState();
  });
}

if (copySqlButton) {
  copySqlButton.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(sqlInput.value);
      flashCopyState();
    } catch (err) {
      updateStatus('Copy to clipboard is unavailable in this context.');
    }
  });
}

// Tab Navigation
document.querySelectorAll('.tab-button').forEach(button => {
  button.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    const tabName = target.dataset.tab;
    if (tabName) {
      switchTab(tabName);
    }
  });
});

// Export Buttons
document.getElementById('export-csv')?.addEventListener('click', () => exportData('csv'));
document.getElementById('export-parquet')?.addEventListener('click', () => exportData('parquet'));
document.getElementById('export-json')?.addEventListener('click', () => exportData('json'));

// Profile Tab
document.getElementById('refresh-profile')?.addEventListener('click', () => refreshDataProfile());

// Query Builder
document.getElementById('build-query-btn')?.addEventListener('click', () => buildQueryFromUI());

// Pivot Table
document.getElementById('generate-pivot-btn')?.addEventListener('click', () => generatePivotTable());

// Charts
document.getElementById('generate-chart-btn')?.addEventListener('click', () => generateChart());

// History
document.getElementById('clear-history-btn')?.addEventListener('click', () => clearHistory());

// Templates
document.getElementById('save-template-btn')?.addEventListener('click', () => saveCurrentQueryAsTemplate());

// --- Core Functions ---

function createDuckDBWorker(workerSource: string, workerUrl: string): { worker: Worker; cleanup: () => void } {
  updateStatus('Creating DuckDB worker from source...');
  const bootstrap = `
      self.window = self;
      self.document = { currentScript: { src: ${JSON.stringify(workerUrl)} } };
  `;
  const blob = new Blob([bootstrap, '\n', workerSource], { type: 'application/javascript' });
  const blobUrl = URL.createObjectURL(blob);
  return {
      worker: new Worker(blobUrl),
      cleanup: () => URL.revokeObjectURL(blobUrl),
  };
}

async function ensureDuckDBInitialized(bundles: duckdb.DuckDBBundles) {
  if (connection) {
    updateStatus('DuckDB ready. Waiting for file data…');
    vscode.postMessage({ command: 'duckdb-ready' });
    return;
  }

  if (!duckdbInitializationPromise) {
    duckdbInitializationPromise = bootstrapDuckDB(bundles).catch((error) => {
      duckdbInitializationPromise = null;
      throw error;
    });
  }

  await duckdbInitializationPromise;
}

async function bootstrapDuckDB(bundles: duckdb.DuckDBBundles) {
  try {
    updateStatus('Selecting DuckDB bundle...');
    const selectedBundle = await duckdb.selectBundle(bundles);

    if (!selectedBundle.mainWorker || typeof selectedBundle.mainWorker !== 'string') {
        throw new Error('Selected bundle has no worker source.');
    }
    if (!selectedBundle.mainModule) {
        throw new Error('Selected bundle has no WASM module URL.');
    }

    const workerUrl = selectedBundle.mainModule.replace('.wasm', '.worker.js');
    const { worker, cleanup } = createDuckDBWorker(selectedBundle.mainWorker, workerUrl);

    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);

    updateStatus('Instantiating DuckDB...');
    await db.instantiate(selectedBundle.mainModule, selectedBundle.pthreadWorker);
    cleanup();

    updateStatus('Opening DuckDB...');
    await db.open({ path: ':memory:' });

    updateStatus('Connecting to DuckDB...');
    connection = await db.connect();

    updateStatus('Installing extensions...');
    await connection.query("INSTALL parquet; LOAD parquet;");
    await connection.query("INSTALL sqlite; LOAD sqlite;");
    await connection.query("INSTALL json; LOAD json;");

    updateStatus('DuckDB ready. Waiting for file data…');
    vscode.postMessage({ command: 'duckdb-ready' });

  } catch (e) {
    reportError(e);
  }
}

async function handleFileLoad(fileName: string, fileData: any) {
  if (!db || !connection) {
    throw new Error('DuckDB is not initialized.');
  }

  const fileBytes = extractFileBytes(fileData);
  if (fileBytes.length === 0) {
    throw new Error('File is empty (0 bytes).');
  }

  const loader = selectLoader(fileName);
  updateStatus(`Preparing ${loader.id.toUpperCase()} data for ${fileName}…`);
  const loadResult = await loader.load(fileName, fileBytes, {
    db,
    connection,
    updateStatus,
  });

  // Store file info
  const rowCountResult = await connection.query(`SELECT COUNT(*) as count FROM ${loadResult.relationIdentifier}`);
  const rowCount = rowCountResult.get(0)?.count || 0;

  loadedFiles.push({
    name: fileName,
    relationName: loadResult.relationIdentifier,
    columns: loadResult.columns,
    rowCount: rowCount
  });

  updateFileList();

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;

  if (controls) controls.style.display = 'flex';
  if (resultsContainer) resultsContainer.style.display = 'block';
  if (fileManager) fileManager.style.display = 'block';

  await runQuery(defaultQuery);

  // Initialize profile data
  if (currentActiveTab === 'profile') {
    await refreshDataProfile();
  }

  // Update query builder columns
  updateQueryBuilderColumns(loadResult.columns);

  // Update chart columns
  updateChartColumns(loadResult.columns);

  // Initialize templates
  renderTemplates();
}

function selectLoader(fileName: string): DataLoader {
  return DATA_LOADERS.find((loader) => loader.canLoad(fileName)) ?? csvLoader;
}

function extractFileBytes(fileData: any): Uint8Array {
  if (fileData instanceof Uint8Array) {
    return fileData;
  }
  if (fileData?.data instanceof ArrayBuffer) {
    return new Uint8Array(fileData.data);
  }
  if (Array.isArray(fileData?.data)) {
    return new Uint8Array(fileData.data);
  }
  if (fileData instanceof ArrayBuffer) {
    return new Uint8Array(fileData);
  }
  if (fileData?.buffer instanceof ArrayBuffer) {
    return new Uint8Array(fileData.buffer);
  }
  throw new Error('Unable to read file bytes from message.');
}

async function runQuery(sql: string) {
  if (!connection) {
    throw new Error("No database connection.");
  }

  updateStatus('Running query...');
  runButton.disabled = true;
  const startTime = performance.now();

  try {
    const result = await connection.query(sql);
    const endTime = performance.now();
    const executionTime = endTime - startTime;

    renderResults(result);

    // Add to history
    queryHistory.unshift({
      query: sql,
      timestamp: Date.now(),
      rowsReturned: result.numRows,
      executionTime: executionTime
    });

    // Keep only last 50 queries
    if (queryHistory.length > 50) {
      queryHistory.length = 50;
    }

    updateHistoryUI();

    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }

  } catch (e) {
    reportError(e);
  } finally {
    runButton.disabled = false;
  }
}

function renderResults(table: Table | null) {
  if (!resultsContainer) {
    return;
  }

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    updateRowCount(0, 0);
    return;
  }

  const rows: TableRow[] = [];
  const columns = table.schema.fields.map((field) => field.name);

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) continue;

    const raw: any[] = [];
    const display: string[] = [];
    for (const field of table.schema.fields) {
      const value = row[field.name];
      raw.push(value);
      display.push(formatCell(value));
    }
    rows.push({ raw, display });
  }

  currentTableData = { columns, rows };
  columnFilters = columns.map(() => '');
  globalFilter = '';
  sortState = { columnIndex: -1, direction: null };
  if (globalSearchInput) {
    globalSearchInput.value = '';
  }

  buildTableSkeleton(columns);
  applyTableState();

  resultsContainer.style.display = 'block';
  resultsContainer.scrollTop = 0;
}

function buildTableSkeleton(columns: string[]) {
  if (!resultsContainer) {
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  headerRow.className = 'column-row';

  columns.forEach((column, index) => {
    const th = document.createElement('th');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'header-button';

    const label = document.createElement('span');
    label.textContent = column;
    const indicator = document.createElement('span');
    indicator.className = 'sort-indicator';

    button.append(label, indicator);
    button.addEventListener('click', () => toggleSort(index));
    th.appendChild(button);
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  const filterRow = document.createElement('tr');
  filterRow.className = 'filter-row';
  columns.forEach((column, index) => {
    const th = document.createElement('th');
    const input = document.createElement('input');
    input.type = 'search';
    input.placeholder = 'Filter';
    input.value = columnFilters[index] ?? '';
    input.setAttribute('aria-label', `Filter column ${column}`);
    input.addEventListener('input', () => {
      columnFilters[index] = input.value;
      applyTableState();
    });
    th.appendChild(input);
    filterRow.appendChild(th);
  });
  thead.appendChild(filterRow);

  const tbody = document.createElement('tbody');
  table.appendChild(thead);
  table.appendChild(tbody);

  resultsContainer.innerHTML = '';
  resultsContainer.appendChild(table);
  tableBodyElement = tbody;
  syncColumnHeaderHeight(headerRow);
}

function applyTableState() {
  if (!currentTableData || !tableBodyElement) {
    return;
  }
  const tbody = tableBodyElement;

  const normalizedGlobal = globalFilter.trim().toLowerCase();
  const normalizedFilters = columnFilters.map((value) => value.trim().toLowerCase());

  let visibleRows = currentTableData.rows.filter((row) => {
    if (normalizedGlobal) {
      const hasMatch = row.display.some((cell) => cell.toLowerCase().includes(normalizedGlobal));
      if (!hasMatch) {
        return false;
      }
    }
    return normalizedFilters.every((filter, idx) => {
      if (!filter) return true;
      return (row.display[idx] ?? '').toLowerCase().includes(filter);
    });
  });

  if (sortState.direction && sortState.columnIndex >= 0) {
    const directionMultiplier = sortState.direction === 'asc' ? 1 : -1;
    const sortIndex = sortState.columnIndex;
    visibleRows = [...visibleRows].sort((a, b) => {
      const comparison = compareValues(
        a.raw[sortIndex],
        b.raw[sortIndex],
        a.display[sortIndex],
        b.display[sortIndex]
      );
      return comparison * directionMultiplier;
    });
  } else {
    visibleRows = [...visibleRows];
  }

  tbody.innerHTML = '';

  if (visibleRows.length === 0) {
    const emptyRow = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = currentTableData.columns.length || 1;
    cell.textContent = 'No rows match the current filters.';
    cell.className = 'empty-row';
    emptyRow.appendChild(cell);
    tbody.appendChild(emptyRow);
  } else {
    visibleRows.forEach((row) => {
      const tr = document.createElement('tr');
      row.display.forEach((value) => {
        const td = document.createElement('td');
        td.textContent = value;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  updateRowCount(visibleRows.length, currentTableData.rows.length);
  refreshSortIndicators();
}

function syncColumnHeaderHeight(headerRow: HTMLTableRowElement) {
  window.requestAnimationFrame(() => {
    const height = headerRow.getBoundingClientRect().height;
    if (height > 0) {
      document.documentElement.style.setProperty('--column-header-height', `${height}px`);
    }
  });
}

function toggleSort(columnIndex: number) {
  if (sortState.columnIndex === columnIndex) {
    if (sortState.direction === 'asc') {
      sortState.direction = 'desc';
    } else if (sortState.direction === 'desc') {
      sortState = { columnIndex: -1, direction: null };
    } else {
      sortState.direction = 'asc';
    }
  } else {
    sortState = { columnIndex, direction: 'asc' };
  }
  applyTableState();
}

function refreshSortIndicators() {
  if (!resultsContainer) {
    return;
  }

  const headerButtons = Array.from(resultsContainer.querySelectorAll<HTMLButtonElement>('.header-button'));
  headerButtons.forEach((button, index) => {
    if (sortState.columnIndex === index && sortState.direction) {
      button.dataset.sort = sortState.direction;
    } else {
      delete button.dataset.sort;
    }
  });
}

function updateRowCount(visible: number, total: number) {
  if (!rowCountLabel) {
    return;
  }
  if (visible === total) {
    rowCountLabel.textContent = `${total.toLocaleString()} rows`;
  } else {
    rowCountLabel.textContent = `${visible.toLocaleString()} of ${total.toLocaleString()} rows`;
  }
}

function compareValues(a: any, b: any, aDisplay: string, bDisplay: string): number {
  if (a === b) return 0;

  const aIsNumber = typeof a === 'number' && Number.isFinite(a);
  const bIsNumber = typeof b === 'number' && Number.isFinite(b);
  if (aIsNumber && bIsNumber) {
    return a < b ? -1 : 1;
  }

  const aIsDate = a instanceof Date;
  const bIsDate = b instanceof Date;
  if (aIsDate && bIsDate) {
    return a.getTime() - b.getTime();
  }

  const textA = (aDisplay ?? '').toLowerCase();
  const textB = (bDisplay ?? '').toLowerCase();
  return textA.localeCompare(textB, undefined, { numeric: true, sensitivity: 'base' });
}

function formatCell(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : '';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

// --- New Feature Functions ---

function switchTab(tabName: string) {
  currentActiveTab = tabName;

  document.querySelectorAll('.tab-button').forEach(btn => {
    btn.classList.toggle('active', btn.getAttribute('data-tab') === tabName);
  });

  document.querySelectorAll('.tab-content').forEach(content => {
    content.classList.toggle('active', content.getAttribute('data-tab') === tabName);
  });

  // Load data when switching to certain tabs
  if (tabName === 'profile' && loadedFiles.length > 0) {
    refreshDataProfile();
  }
}

function updateFileList() {
  if (!fileList) return;

  fileList.innerHTML = '';
  loadedFiles.forEach((file, index) => {
    const chip = document.createElement('div');
    chip.className = 'file-chip';
    chip.innerHTML = `
      <span>${file.name} (${file.rowCount.toLocaleString()} rows)</span>
      <button onclick="removeFile(${index})" title="Remove file">×</button>
    `;
    fileList.appendChild(chip);
  });
}

async function refreshDataProfile() {
  if (!connection || loadedFiles.length === 0) return;

  const file = loadedFiles[loadedFiles.length - 1]; // Profile the most recent file
  updateStatus('Generating data profile...');

  try {
    // Overview statistics
    const overviewStats = document.getElementById('overview-stats');
    if (overviewStats) {
      overviewStats.innerHTML = `
        <div class="stat-card">
          <div class="stat-label">Total Rows</div>
          <div class="stat-value">${file.rowCount.toLocaleString()}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Total Columns</div>
          <div class="stat-value">${file.columns.length}</div>
        </div>
      `;
    }

    // Column profiles
    const columnProfiles = document.getElementById('column-profiles');
    if (columnProfiles) {
      columnProfiles.innerHTML = '';

      for (const column of file.columns) {
        const profileDiv = document.createElement('div');
        profileDiv.className = 'column-profile';

        // Get column statistics
        const stats = await connection.query(`
          SELECT
            COUNT(*) as total,
            COUNT(DISTINCT "${column}") as unique_values,
            COUNT("${column}") as non_null,
            COUNT(*) - COUNT("${column}") as null_count
          FROM ${file.relationName}
        `);

        const statsRow = stats.get(0);
        const nullPercentage = ((statsRow.null_count / statsRow.total) * 100).toFixed(1);

        profileDiv.innerHTML = `
          <div class="column-name">${column}</div>
          <div class="profile-stats">
            <div class="profile-stat">
              <span class="profile-stat-label">Unique Values:</span>
              <span>${statsRow.unique_values.toLocaleString()}</span>
            </div>
            <div class="profile-stat">
              <span class="profile-stat-label">Non-Null:</span>
              <span>${statsRow.non_null.toLocaleString()}</span>
            </div>
            <div class="profile-stat">
              <span class="profile-stat-label">Null Count:</span>
              <span>${statsRow.null_count.toLocaleString()} (${nullPercentage}%)</span>
            </div>
          </div>
        `;

        columnProfiles.appendChild(profileDiv);
      }
    }

    if (statusWrapper) statusWrapper.style.display = 'none';
  } catch (e) {
    reportError(e);
  }
}

async function exportData(format: 'csv' | 'parquet' | 'json') {
  if (!connection || !currentTableData) {
    updateStatus('No data to export');
    return;
  }

  try {
    updateStatus(`Exporting as ${format.toUpperCase()}...`);

    let exportSQL = '';
    const lastQuery = sqlInput.value || 'SELECT * FROM ' + (loadedFiles[0]?.relationName || 'data');

    switch (format) {
      case 'csv':
        exportSQL = `COPY (${lastQuery}) TO '/tmp/export.csv' (HEADER, DELIMITER ',')`;
        break;
      case 'parquet':
        exportSQL = `COPY (${lastQuery}) TO '/tmp/export.parquet' (FORMAT PARQUET)`;
        break;
      case 'json':
        exportSQL = `COPY (${lastQuery}) TO '/tmp/export.json' (FORMAT JSON)`;
        break;
    }

    // Note: In browser WASM, we can't write to filesystem directly
    // Instead, we'll get the data and trigger a download
    const result = await connection.query(lastQuery);

    let blob: Blob;
    let filename: string;

    if (format === 'csv') {
      const csvData = tableToCSV(result);
      blob = new Blob([csvData], { type: 'text/csv' });
      filename = 'export.csv';
    } else if (format === 'json') {
      const jsonData = tableToJSON(result);
      blob = new Blob([jsonData], { type: 'application/json' });
      filename = 'export.json';
    } else {
      updateStatus('Parquet export not yet supported in browser');
      return;
    }

    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    updateStatus(`Exported as ${format.toUpperCase()}`);
    setTimeout(() => {
      if (statusWrapper) statusWrapper.style.display = 'none';
    }, 2000);

  } catch (e) {
    reportError(e);
  }
}

function tableToCSV(table: Table): string {
  const columns = table.schema.fields.map(f => f.name);
  const rows: string[] = [columns.join(',')];

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) continue;
    const values = columns.map(col => {
      const val = row[col];
      const str = formatCell(val);
      // Escape CSV values
      return str.includes(',') || str.includes('"') || str.includes('\n')
        ? `"${str.replace(/"/g, '""')}"`
        : str;
    });
    rows.push(values.join(','));
  }

  return rows.join('\n');
}

function tableToJSON(table: Table): string {
  const columns = table.schema.fields.map(f => f.name);
  const rows: any[] = [];

  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i);
    if (!row) continue;
    const obj: any = {};
    columns.forEach(col => {
      obj[col] = row[col];
    });
    rows.push(obj);
  }

  return JSON.stringify(rows, null, 2);
}

function updateQueryBuilderColumns(columns: string[]) {
  const selectColumns = document.getElementById('select-columns');
  const groupbyColumns = document.getElementById('groupby-columns');
  const orderbyColumns = document.getElementById('orderby-columns');

  if (selectColumns) {
    selectColumns.innerHTML = '';
    columns.forEach(col => {
      const tag = document.createElement('div');
      tag.className = 'column-tag';
      tag.textContent = col;
      tag.dataset.column = col;
      tag.addEventListener('click', () => tag.classList.toggle('selected'));
      selectColumns.appendChild(tag);
    });
  }

  if (groupbyColumns) {
    groupbyColumns.innerHTML = '';
    columns.forEach(col => {
      const tag = document.createElement('div');
      tag.className = 'column-tag';
      tag.textContent = col;
      tag.dataset.column = col;
      tag.addEventListener('click', () => tag.classList.toggle('selected'));
      groupbyColumns.appendChild(tag);
    });
  }

  if (orderbyColumns) {
    orderbyColumns.innerHTML = '';
    columns.forEach(col => {
      const tag = document.createElement('div');
      tag.className = 'column-tag';
      tag.textContent = col;
      tag.dataset.column = col;
      tag.addEventListener('click', () => tag.classList.toggle('selected'));
      orderbyColumns.appendChild(tag);
    });
  }
}

function buildQueryFromUI() {
  if (loadedFiles.length === 0) return;

  const tableName = loadedFiles[loadedFiles.length - 1].relationName;

  // Get selected columns
  const selectCols = Array.from(document.querySelectorAll('#select-columns .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);
  const selectClause = selectCols.length > 0 ? selectCols.join(', ') : '*';

  // Build query
  let query = `SELECT ${selectClause}\nFROM ${tableName}`;

  // Group by
  const groupbyCols = Array.from(document.querySelectorAll('#groupby-columns .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);
  if (groupbyCols.length > 0) {
    query += `\nGROUP BY ${groupbyCols.join(', ')}`;
  }

  // Order by
  const orderbyCols = Array.from(document.querySelectorAll('#orderby-columns .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);
  if (orderbyCols.length > 0) {
    query += `\nORDER BY ${orderbyCols.join(', ')}`;
  }

  // Limit
  const limitInput = document.getElementById('limit-input') as HTMLInputElement;
  if (limitInput && limitInput.value) {
    query += `\nLIMIT ${limitInput.value}`;
  }

  sqlInput.value = query;
  switchTab('data');
}

async function generatePivotTable() {
  if (!connection || loadedFiles.length === 0) return;

  const tableName = loadedFiles[loadedFiles.length - 1].relationName;

  const rowCols = Array.from(document.querySelectorAll('#pivot-rows .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);
  const colCols = Array.from(document.querySelectorAll('#pivot-columns .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);
  const valCols = Array.from(document.querySelectorAll('#pivot-values .column-tag.selected'))
    .map(el => (el as HTMLElement).dataset.column);

  if (rowCols.length === 0 || colCols.length === 0 || valCols.length === 0) {
    updateStatus('Please select columns for rows, columns, and values');
    return;
  }

  // Build pivot query
  const pivotQuery = `
    PIVOT ${tableName}
    ON ${colCols[0]}
    USING SUM(${valCols[0]})
    GROUP BY ${rowCols.join(', ')}
  `;

  sqlInput.value = pivotQuery;
  switchTab('data');
  await runQuery(pivotQuery);
}

function updateChartColumns(columns: string[]) {
  const xSelect = document.getElementById('chart-x-column') as HTMLSelectElement;
  const ySelect = document.getElementById('chart-y-column') as HTMLSelectElement;

  if (xSelect) {
    xSelect.innerHTML = columns.map(col => `<option value="${col}">${col}</option>`).join('');
  }
  if (ySelect) {
    ySelect.innerHTML = columns.map(col => `<option value="${col}">${col}</option>`).join('');
  }
}

async function generateChart() {
  if (!connection || loadedFiles.length === 0) return;

  const chartType = (document.getElementById('chart-type') as HTMLSelectElement).value;
  const xColumn = (document.getElementById('chart-x-column') as HTMLSelectElement).value;
  const yColumn = (document.getElementById('chart-y-column') as HTMLSelectElement).value;
  const limit = (document.getElementById('chart-limit') as HTMLInputElement).value || '100';

  const tableName = loadedFiles[loadedFiles.length - 1].relationName;
  const query = `SELECT "${xColumn}", "${yColumn}" FROM ${tableName} LIMIT ${limit}`;

  try {
    const result = await connection.query(query);

    // Simple ASCII chart rendering
    const chartContainer = document.getElementById('chart-container');
    if (!chartContainer) return;

    chartContainer.innerHTML = '<div class="empty-state">Chart visualization would appear here.<br>Note: Full chart library integration recommended for production use.</div>';

    // For now, show data table
    const table = document.createElement('table');
    table.style.width = '100%';

    const thead = document.createElement('thead');
    thead.innerHTML = `<tr><th>${xColumn}</th><th>${yColumn}</th></tr>`;
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (let i = 0; i < Math.min(result.numRows, 20); i++) {
      const row = result.get(i);
      if (!row) continue;
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${formatCell(row[xColumn])}</td><td>${formatCell(row[yColumn])}</td>`;
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);

    chartContainer.appendChild(table);

  } catch (e) {
    reportError(e);
  }
}

function updateHistoryUI() {
  const historyList = document.getElementById('history-list');
  if (!historyList) return;

  if (queryHistory.length === 0) {
    historyList.innerHTML = '<div class="empty-state">No queries yet. Run a query to see it here.</div>';
    return;
  }

  historyList.innerHTML = '';
  queryHistory.forEach((item, index) => {
    const historyItem = document.createElement('div');
    historyItem.className = 'history-item';
    historyItem.innerHTML = `
      <div class="history-query">${item.query}</div>
      <div class="history-meta">
        <span>${new Date(item.timestamp).toLocaleString()}</span>
        <span>${item.rowsReturned} rows • ${item.executionTime.toFixed(2)}ms</span>
      </div>
    `;
    historyItem.addEventListener('click', () => {
      sqlInput.value = item.query;
      switchTab('data');
    });
    historyList.appendChild(historyItem);
  });
}

function clearHistory() {
  queryHistory.length = 0;
  updateHistoryUI();
}

function renderTemplates() {
  const templateGrid = document.getElementById('template-grid');
  if (!templateGrid) return;

  templateGrid.innerHTML = '';
  queryTemplates.forEach(template => {
    const card = document.createElement('div');
    card.className = 'template-card';
    card.innerHTML = `
      <div class="template-title">${template.name}</div>
      <div class="template-desc">${template.description}</div>
    `;
    card.addEventListener('click', () => {
      let sql = template.sql;

      // Replace placeholders
      if (loadedFiles.length > 0) {
        const file = loadedFiles[loadedFiles.length - 1];
        sql = sql.replace(/{table}/g, file.relationName);
        if (file.columns.length > 0) {
          sql = sql.replace(/{column}/g, file.columns[0]);
          sql = sql.replace(/{date_column}/g, file.columns[0]);
          sql = sql.replace(/{order_column}/g, file.columns[0]);
          sql = sql.replace(/{value_column}/g, file.columns[0]);
        }
      }

      sqlInput.value = sql;
      switchTab('data');
    });
    templateGrid.appendChild(card);
  });
}

function saveCurrentQueryAsTemplate() {
  const name = prompt('Template name:');
  if (!name) return;

  const description = prompt('Description:');
  if (!description) return;

  queryTemplates.push({
    name,
    description,
    sql: sqlInput.value
  });

  renderTemplates();
  updateStatus('Template saved!');
  setTimeout(() => {
    if (statusWrapper) statusWrapper.style.display = 'none';
  }, 2000);
}

// --- Helpers ---
function flashCopyState() {
  if (!copySqlButton) {
    return;
  }
  const originalLabel = copySqlButton.textContent ?? 'Copy SQL';
  copySqlButton.textContent = 'Copied!';
  copySqlButton.disabled = true;
  if (copyTimeoutHandle) {
    window.clearTimeout(copyTimeoutHandle);
  }
  copyTimeoutHandle = window.setTimeout(() => {
    copySqlButton!.textContent = originalLabel;
    copySqlButton!.disabled = false;
  }, 1200);
}

function updateStatus(message: string) {
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = message;
    status.classList.remove('error');
  }
}

function reportError(e: any) {
  const message = e instanceof Error ? e.message : String(e);

  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = `Error: ${message}`;
    status.classList.add('error');
  }
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
