import * as duckdb from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow';
import { csvLoader } from './loaders/csvLoader';
import { arrowLoader } from './loaders/arrowLoader';
import { parquetLoader } from './loaders/parquetLoader';
import { ColumnDetail, DataLoader } from './loaders/types';
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
const insightsSummary = document.getElementById('insights-summary');
const insightsColumns = document.getElementById('insights-columns');
const smartQueryList = document.getElementById('smart-query-list');
const queryHistoryList = document.getElementById('query-history-list');
const clearHistoryButton = document.getElementById('clear-history') as HTMLButtonElement | null;
const historyEmptyState = document.getElementById('history-empty-state');
const downloadResultsButton = document.getElementById('download-results') as HTMLButtonElement | null;

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface QueryHistoryItem {
  id: string;
  sql: string;
  timestamp: number;
  rowCount: number | null;
}

interface SmartQuery {
  id: string;
  label: string;
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
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];
let currentRelationIdentifier: string | null = null;
let currentRelationName: string | null = null;
let currentColumnDetails: ColumnDetail[] = [];
let datasetTotalRowCount: number | null = null;
let currentFileSizeBytes: number | null = null;
let smartQueries: SmartQuery[] = [];
let queryHistory: QueryHistoryItem[] = [];
const MAX_HISTORY_ITEMS = 15;

// --- Event Listeners (Moved to top) ---

// Listen for messages from the extension
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

// Listen for the "Run" button click
runButton.addEventListener('click', () => {
  runQuery(sqlInput.value).catch(reportError);
});

// Allow Cmd/Ctrl + Enter to run the query
sqlInput.addEventListener('keydown', (event: KeyboardEvent) => {
  const isSubmitShortcut = event.key === 'Enter' && (event.metaKey || event.ctrlKey);
  if (isSubmitShortcut) {
    event.preventDefault();
    runQuery(sqlInput.value).catch(reportError);
  }
});

// Global search box to filter visible rows
if (globalSearchInput) {
  globalSearchInput.addEventListener('input', () => {
    globalFilter = globalSearchInput.value;
    applyTableState();
  });
}

// Copy SQL to the clipboard for quick sharing
if (copySqlButton) {
  copySqlButton.addEventListener('click', async () => {
    try {
      const clipboard = navigator.clipboard;
      if (!clipboard) {
        updateStatus('Clipboard access is not available in this environment.');
        return;
      }
      await clipboard.writeText(sqlInput.value);
      flashCopyState();
    } catch (err) {
      updateStatus('Copy to clipboard is unavailable in this context.');
      console.warn('[Webview] Clipboard copy failed', err);
    }
  });
}

if (smartQueryList) {
  smartQueryList.addEventListener('click', (event) => {
    const target = event.target as HTMLElement;
    const button = target.closest<HTMLButtonElement>('[data-sql]');
    if (!button) {
      return;
    }
    const sql = button.dataset.sql;
    if (!sql) {
      return;
    }
    sqlInput.value = sql;
    runQuery(sql).catch(reportError);
  });
}

if (queryHistoryList) {
  queryHistoryList.addEventListener('click', (event) => {
    const target = event.target as HTMLElement;
    const button = target.closest<HTMLButtonElement>('[data-history-action]');
    if (!button) {
      return;
    }
    const id = button.dataset.id;
    if (!id) {
      return;
    }
    const entry = queryHistory.find((item) => item.id === id);
    if (!entry) {
      return;
    }
    if (button.dataset.historyAction === 'use') {
      sqlInput.value = entry.sql;
      sqlInput.focus();
    }
    if (button.dataset.historyAction === 'run') {
      sqlInput.value = entry.sql;
      runQuery(entry.sql).catch(reportError);
    }
  });
}

if (clearHistoryButton) {
  clearHistoryButton.addEventListener('click', () => {
    clearQueryHistory();
  });
}

if (downloadResultsButton) {
  downloadResultsButton.addEventListener('click', () => {
    downloadResultsAsCsv();
  });
}

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

  currentRelationIdentifier = loadResult.relationIdentifier;
  currentRelationName = loadResult.relationName;
  currentColumnDetails = loadResult.columnDetails ?? loadResult.columns.map((name) => ({ name, type: 'unknown' }));
  currentFileSizeBytes = fileBytes.byteLength;
  datasetTotalRowCount = null;
  renderColumnDetails();

  smartQueries = buildSmartQueries(currentColumnDetails, loadResult.relationIdentifier);
  renderSmartQueries();
  renderInsightsSummary();
  void refreshDatasetInsights();

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;

  if (controls) {
    controls.style.display = 'flex';
  }
  if (resultsContainer) {
    resultsContainer.style.display = 'block';
  }

  await runQuery(defaultQuery);
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
  
  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const result = await connection.query(sql);
    renderResults(result);
    recordQueryHistory(sql, result ? result.numRows : null);

    // --- CHANGE ---
    // Hide the status bar on success
    if (statusWrapper) {
      statusWrapper.style.display = 'none';
    }
    // ---

  } catch (e) {
    reportError(e); // reportError will show the status bar
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
    if (!row) {
      continue;
    }

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
    const detail = currentColumnDetails.find((item) => item.name === column);
    if (detail) {
      const meta = document.createElement('div');
      meta.className = 'header-meta';
      meta.textContent = detail.type;
      th.appendChild(meta);
    }
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
      if (!filter) {
        return true;
      }
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
  const formattedVisible = formatNumber(visible);
  const formattedTotal = formatNumber(total);
  let message = `Showing ${formattedVisible} of ${formattedTotal} row${total === 1 ? '' : 's'} from the last query.`;

  if (datasetTotalRowCount !== null && datasetTotalRowCount >= 0) {
    const formattedDataset = formatNumber(datasetTotalRowCount);
    if (datasetTotalRowCount !== total) {
      message += ` Dataset holds ${formattedDataset} row${datasetTotalRowCount === 1 ? '' : 's'}.`;
    } else {
      message += ` Dataset contains ${formattedDataset} row${datasetTotalRowCount === 1 ? '' : 's'} overall.`;
    }
  }

  rowCountLabel.textContent = message;
}

function compareValues(a: any, b: any, aDisplay: string, bDisplay: string): number {
  if (a === b) {
    return 0;
  }

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

function renderColumnDetails() {
  if (!insightsColumns) {
    return;
  }

  insightsColumns.innerHTML = '';

  if (!currentColumnDetails.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'Load a file to inspect its column blueprint.';
    insightsColumns.appendChild(empty);
    return;
  }

  const list = document.createElement('ul');
  list.className = 'column-overview';
  const maxDisplay = 20;

  currentColumnDetails.slice(0, maxDisplay).forEach((detail) => {
    const item = document.createElement('li');
    item.className = 'column-overview-item';

    const name = document.createElement('span');
    name.className = 'column-overview-name';
    name.textContent = detail.name;

    const type = document.createElement('span');
    type.className = 'column-type-badge';
    type.textContent = detail.type;

    item.append(name, type);
    list.appendChild(item);
  });

  insightsColumns.appendChild(list);

  if (currentColumnDetails.length > maxDisplay) {
    const extra = document.createElement('div');
    extra.className = 'column-footnote';
    extra.textContent = `+${formatNumber(currentColumnDetails.length - maxDisplay)} more columns not shown here.`;
    insightsColumns.appendChild(extra);
  }
}

function buildSmartQueries(columns: ColumnDetail[], relationIdentifier: string): SmartQuery[] {
  if (!columns.length) {
    return [];
  }

  const relation = relationIdentifier;
  const queries: SmartQuery[] = [
    {
      id: 'total-row-count',
      label: 'Total row count',
      description: 'Measure the entire dataset instantly.',
      sql: `SELECT COUNT(*) AS total_rows\nFROM ${relation};`,
    },
    {
      id: 'random-sample',
      label: 'Random 50 rows',
      description: 'Dive into a surprise sample to catch outliers.',
      sql: `SELECT *\nFROM ${relation}\nUSING SAMPLE 50 ROWS;`,
    },
  ];

  const numericColumns = columns.filter((col) => isNumericType(col.type)).slice(0, 2);
  numericColumns.forEach((col, index) => {
    const identifier = formatSqlIdentifier(col.name);
    const aliasBase = sanitizeAlias(col.name || `metric_${index}`);
    queries.push({
      id: `profile-${aliasBase}`,
      label: `Profile metrics: ${col.name}`,
      description: 'Min, max, average, and deviation in one blast.',
      sql: `SELECT\n  MIN(${identifier}) AS min_${aliasBase},\n  MAX(${identifier}) AS max_${aliasBase},\n  AVG(${identifier}) AS avg_${aliasBase},\n  STDDEV_POP(${identifier}) AS stddev_${aliasBase}\nFROM ${relation};`,
    });
  });

  const textColumns = columns.filter((col) => isTextType(col.type)).slice(0, 2);
  textColumns.forEach((col, index) => {
    const identifier = formatSqlIdentifier(col.name);
    const aliasBase = sanitizeAlias(col.name || `category_${index}`);
    queries.push({
      id: `top-${aliasBase}`,
      label: `Top values: ${col.name}`,
      description: 'Surface the dominant categories with frequencies.',
      sql: `SELECT\n  ${identifier} AS value,\n  COUNT(*) AS frequency\nFROM ${relation}\nGROUP BY ${identifier}\nORDER BY frequency DESC\nLIMIT 25;`,
    });
  });

  const temporalColumn = columns.find((col) => isTemporalType(col.type));
  if (temporalColumn) {
    const identifier = formatSqlIdentifier(temporalColumn.name);
    const aliasBase = sanitizeAlias(temporalColumn.name || 'time');
    queries.push({
      id: `timeline-${aliasBase}`,
      label: `Timeline: ${temporalColumn.name}`,
      description: 'Group records by day to reveal trends.',
      sql: `SELECT\n  DATE_TRUNC('day', ${identifier}) AS day_bucket,\n  COUNT(*) AS rows\nFROM ${relation}\nGROUP BY day_bucket\nORDER BY day_bucket;`,
    });
  }

  return queries;
}

function renderSmartQueries() {
  if (!smartQueryList) {
    return;
  }

  smartQueryList.innerHTML = '';

  if (!smartQueries.length) {
    const empty = document.createElement('li');
    empty.className = 'smart-query-empty';
    empty.textContent = 'Load a dataset to unlock smart query recipes.';
    smartQueryList.appendChild(empty);
    return;
  }

  smartQueries.forEach((query) => {
    const item = document.createElement('li');
    item.className = 'smart-query-item';

    const textWrapper = document.createElement('div');
    textWrapper.className = 'smart-query-text';

    const title = document.createElement('strong');
    title.textContent = query.label;

    const description = document.createElement('p');
    description.textContent = query.description;

    textWrapper.append(title, description);

    const runButton = document.createElement('button');
    runButton.type = 'button';
    runButton.className = 'smart-query-run';
    runButton.dataset.sql = query.sql;
    runButton.textContent = 'Run now';

    item.append(textWrapper, runButton);
    smartQueryList.appendChild(item);
  });
}

async function refreshDatasetInsights() {
  if (!connection || !currentRelationIdentifier) {
    return;
  }

  try {
    const result = await connection.query(`SELECT COUNT(*) AS total_rows FROM ${currentRelationIdentifier};`);
    const rows = result.toArray();
    const value = rows.length > 0 ? Number(rows[0]?.total_rows) : 0;
    datasetTotalRowCount = Number.isFinite(value) ? value : null;
  } catch (error) {
    console.warn('[Webview] Unable to compute dataset row count', error);
    datasetTotalRowCount = null;
  }

  renderInsightsSummary();
}

function renderInsightsSummary() {
  if (!insightsSummary) {
    return;
  }

  insightsSummary.innerHTML = '';

  if (!currentRelationName) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'Open a file to unlock instant DuckDB insights.';
    insightsSummary.appendChild(empty);
    return;
  }

  const cards = [
    {
      label: 'Rows detected',
      value: datasetTotalRowCount !== null ? formatNumber(datasetTotalRowCount) : 'Crunching…',
      hint: datasetTotalRowCount !== null ? 'Total rows materialized inside DuckDB.' : 'DuckDB is scanning the dataset…',
    },
    {
      label: 'Columns',
      value: formatNumber(currentColumnDetails.length),
      hint: 'Fields exposed for SQL exploration.',
    },
  ];

  if (currentFileSizeBytes !== null) {
    cards.push({
      label: 'File size loaded',
      value: formatBytes(currentFileSizeBytes),
      hint: 'Raw bytes streamed into DuckDB in this session.',
    });
  }

  const numericCount = currentColumnDetails.filter((col) => isNumericType(col.type)).length;
  const textCount = currentColumnDetails.filter((col) => isTextType(col.type)).length;
  const temporalCount = currentColumnDetails.filter((col) => isTemporalType(col.type)).length;

  cards.push({
    label: 'Column DNA',
    value: `${formatNumber(numericCount)} numeric · ${formatNumber(textCount)} text · ${formatNumber(temporalCount)} time`,
    hint: 'DuckDB automatically tunes execution for these data families.',
  });

  cards.forEach((card) => {
    const cardElement = document.createElement('div');
    cardElement.className = 'insight-card';

    const value = document.createElement('div');
    value.className = 'insight-value';
    value.textContent = card.value;

    const label = document.createElement('div');
    label.className = 'insight-label';
    label.textContent = card.label;

    const hint = document.createElement('div');
    hint.className = 'insight-hint';
    hint.textContent = card.hint;

    cardElement.append(value, label, hint);
    insightsSummary.appendChild(cardElement);
  });
}

function isNumericType(type: string): boolean {
  const normalized = type.toLowerCase();
  return /int|decimal|double|float|numeric|real/.test(normalized);
}

function isTextType(type: string): boolean {
  const normalized = type.toLowerCase();
  return /string|varchar|text|char/.test(normalized);
}

function isTemporalType(type: string): boolean {
  const normalized = type.toLowerCase();
  return /date|time|timestamp/.test(normalized);
}

function formatSqlIdentifier(identifier: string): string {
  const SIMPLE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
  if (SIMPLE_IDENTIFIER.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
}

function sanitizeAlias(identifier: string): string {
  const sanitized = identifier.replace(/[^A-Za-z0-9_]/g, '_');
  return sanitized.length ? sanitized.toLowerCase() : 'col';
}

function recordQueryHistory(sql: string, rowCount: number | null) {
  const trimmed = sql.trim();
  if (!trimmed) {
    return;
  }

  const existingIndex = queryHistory.findIndex((entry) => entry.sql === trimmed);
  if (existingIndex >= 0) {
    queryHistory.splice(existingIndex, 1);
  }

  const entry: QueryHistoryItem = {
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    sql: trimmed,
    timestamp: Date.now(),
    rowCount: typeof rowCount === 'number' && Number.isFinite(rowCount) ? rowCount : null,
  };

  queryHistory.unshift(entry);
  if (queryHistory.length > MAX_HISTORY_ITEMS) {
    queryHistory = queryHistory.slice(0, MAX_HISTORY_ITEMS);
  }

  renderQueryHistory();
  persistState();
}

function renderQueryHistory() {
  if (!queryHistoryList) {
    return;
  }

  queryHistoryList.innerHTML = '';

  if (!queryHistory.length) {
    if (historyEmptyState) {
      historyEmptyState.style.display = 'block';
    }
    return;
  }

  if (historyEmptyState) {
    historyEmptyState.style.display = 'none';
  }

  queryHistory.forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'query-history-item';

    const text = document.createElement('div');
    text.className = 'query-history-text';

    const code = document.createElement('code');
    code.className = 'query-snippet';
    code.textContent = summarizeSql(entry.sql);

    const meta = document.createElement('span');
    meta.className = 'query-meta';
    const time = formatRelativeTime(entry.timestamp);
    const rowInfo = entry.rowCount !== null ? `${formatNumber(entry.rowCount)} row${entry.rowCount === 1 ? '' : 's'}` : 'Row count n/a';
    meta.textContent = `${time} • ${rowInfo}`;

    text.append(code, meta);

    const actions = document.createElement('div');
    actions.className = 'query-history-actions';

    const loadButton = document.createElement('button');
    loadButton.type = 'button';
    loadButton.dataset.historyAction = 'use';
    loadButton.dataset.id = entry.id;
    loadButton.textContent = 'Load';

    const runButton = document.createElement('button');
    runButton.type = 'button';
    runButton.dataset.historyAction = 'run';
    runButton.dataset.id = entry.id;
    runButton.textContent = 'Run';

    actions.append(loadButton, runButton);

    item.append(text, actions);
    queryHistoryList.appendChild(item);
  });
}

function summarizeSql(sql: string): string {
  const singleLine = sql.replace(/\s+/g, ' ').trim();
  if (singleLine.length <= 140) {
    return singleLine;
  }
  return `${singleLine.slice(0, 137)}…`;
}

function clearQueryHistory() {
  queryHistory = [];
  renderQueryHistory();
  persistState();
}

function persistState() {
  try {
    vscode.setState?.({ queryHistory });
  } catch (error) {
    console.warn('[Webview] Failed to persist state', error);
  }
}

function hydrateState() {
  try {
    const persisted = vscode.getState?.() as { queryHistory?: QueryHistoryItem[] } | undefined;
    if (persisted?.queryHistory && Array.isArray(persisted.queryHistory)) {
      queryHistory = persisted.queryHistory
        .filter((entry): entry is QueryHistoryItem => typeof entry?.sql === 'string')
        .slice(0, MAX_HISTORY_ITEMS)
        .map((entry) => ({
          id: typeof entry.id === 'string' ? entry.id : `${Date.now()}-${Math.random().toString(16).slice(2)}`,
          sql: entry.sql,
          timestamp: typeof entry.timestamp === 'number' ? entry.timestamp : Date.now(),
          rowCount: typeof entry.rowCount === 'number' ? entry.rowCount : null,
        }));
      renderQueryHistory();
    }
  } catch (error) {
    console.warn('[Webview] Failed to hydrate state', error);
  }
}

function formatRelativeTime(timestamp: number): string {
  const diff = Date.now() - timestamp;
  const seconds = Math.max(1, Math.floor(diff / 1000));
  if (seconds < 5) {
    return 'just now';
  }
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  const days = Math.floor(hours / 24);
  if (days < 7) {
    return `${days}d ago`;
  }
  const weeks = Math.floor(days / 7);
  if (weeks < 5) {
    return `${weeks}w ago`;
  }
  const months = Math.floor(days / 30);
  if (months < 12) {
    return `${months}mo ago`;
  }
  const years = Math.floor(days / 365);
  return `${years}y ago`;
}

function formatNumber(value: number): string {
  return Number.isFinite(value) ? value.toLocaleString() : '—';
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return '—';
  }
  if (bytes === 0) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, exponent);
  return `${value.toFixed(exponent === 0 ? 0 : 2)} ${units[exponent]}`;
}

function downloadResultsAsCsv() {
  if (!currentTableData || !currentTableData.rows.length) {
    updateStatus('Run a query before exporting results.');
    return;
  }

  const header = currentTableData.columns.map(escapeCsvValue).join(',');
  const rows = currentTableData.rows.map((row) => row.display.map(escapeCsvValue).join(','));
  const csv = [header, ...rows].join('\n');

  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  const fileBase = (currentRelationName ?? 'duckdb-results').replace(/[^A-Za-z0-9-_]/g, '_');
  anchor.download = `${fileBase || 'duckdb-results'}.csv`;
  anchor.style.display = 'none';
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
  updateStatus('Latest preview exported as CSV.');
}

function escapeCsvValue(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

// ---
// Helpers
// ---
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
  // Always make the status bar visible when updating
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = message;
    status.classList.remove('error'); // Remove error style if it was there
  }
}
function reportError(e: any) {
  const message = e instanceof Error ? e.message : String(e);

  // Always make the status bar visible for errors
  if (statusWrapper) {
    statusWrapper.style.display = 'block';
  }
  if (status) {
    status.textContent = `Error: ${message}`;
    status.classList.add('error'); // Add a red error style
  }
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension to start the handshake
hydrateState();
renderQueryHistory();
renderSmartQueries();
renderColumnDetails();
renderInsightsSummary();
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
