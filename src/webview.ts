import * as duckdb from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow';
import { csvLoader } from './loaders/csvLoader';
import { arrowLoader } from './loaders/arrowLoader';
import { parquetLoader } from './loaders/parquetLoader';
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
const statusIcon = document.querySelector('.status-icon');
const globalSearchInput = document.getElementById('global-search') as HTMLInputElement;
const rowCountLabel = document.getElementById('row-count');
const insightCards = document.getElementById('insight-cards');
const schemaList = document.getElementById('schema-list');
const historyList = document.getElementById('history-list');
const downloadButton = document.getElementById('download-results') as HTMLButtonElement | null;

const MAX_HISTORY_ITEMS = 8;
const UNIQUE_SAMPLE_LIMIT = 1500;
const SAMPLE_VALUE_LIMIT = 3;
const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 });
const integerFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const dateTimeFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short',
});
const friendlyTypeLabels: Record<ObservedType, string> = {
  number: 'Numeric',
  string: 'Text',
  boolean: 'Boolean',
  date: 'Date/Time',
  object: 'Struct',
  mixed: 'Mixed',
};

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

type ObservedType = 'number' | 'string' | 'boolean' | 'date' | 'object' | 'mixed';

interface ColumnSummary {
  name: string;
  declaredType: string;
  observedType: ObservedType;
  nullCount: number;
  nonNullCount: number;
  uniqueValues: number;
  sampleValues: string[];
  numeric?: { min: number; max: number; avg: number };
  dateRange?: { min: Date; max: Date };
}

interface TableData {
  columns: string[];
  types: string[];
  rows: TableRow[];
}

let db: duckdb.AsyncDuckDB | null = null;
let connection: duckdb.AsyncDuckDBConnection | null = null;
let currentTableData: TableData | null = null;
let columnFilters: string[] = [];
let globalFilter = '';
let sortState: { columnIndex: number; direction: SortDirection } = { columnIndex: -1, direction: null };
let tableBodyElement: HTMLTableSectionElement | null = null;
let copyTimeoutHandle: number | null = null;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];
let queryHistory: string[] = [];

// --- Event Listeners (Moved to top) ---

// Listen for messages from the extension
window.addEventListener('message', (event: any) => {
  const message = event.data;
  if (message.command === 'init') {
    bootstrapDuckDB(message.bundles).catch(reportError);
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

if (downloadButton) {
  downloadButton.addEventListener('click', () => {
    exportVisibleRowsToCSV();
  });
}

renderQueryHistory();
updateInsights(null);

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
    rememberQuery(sql);

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
    setDownloadAvailability(false);
    updateInsights(null);
    return;
  }

  const rows: TableRow[] = [];
  const columns = table.schema.fields.map((field) => field.name);
  const types = table.schema.fields.map((field) => field.type?.toString() ?? '');

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

  currentTableData = { columns, types, rows };
  columnFilters = columns.map(() => '');
  globalFilter = '';
  sortState = { columnIndex: -1, direction: null };
  if (globalSearchInput) {
    globalSearchInput.value = '';
  }

  buildTableSkeleton(columns);
  applyTableState();
  updateInsights(currentTableData);

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
  const visibleRows = getVisibleRows();

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
  setDownloadAvailability(visibleRows.length > 0);
  refreshSortIndicators();
}

function getVisibleRows(): TableRow[] {
  if (!currentTableData) {
    return [];
  }

  const normalizedGlobal = globalFilter.trim().toLowerCase();
  const normalizedFilters = columnFilters.map((value) => value.trim().toLowerCase());

  let filteredRows = currentTableData.rows.filter((row) => {
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
    filteredRows = [...filteredRows].sort((a, b) => {
      const comparison = compareValues(
        a.raw[sortIndex],
        b.raw[sortIndex],
        a.display[sortIndex],
        b.display[sortIndex]
      );
      return comparison * directionMultiplier;
    });
  } else {
    filteredRows = [...filteredRows];
  }

  return filteredRows;
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
  const filtersApplied = Boolean(globalFilter.trim()) || columnFilters.some((value) => value.trim());
  const visibleLabel = `${integerFormatter.format(visible)} row${visible === 1 ? '' : 's'} visible`;
  const totalLabel = `${integerFormatter.format(total)} total`;
  const filterSuffix = filtersApplied && visible !== total ? ' • filters active' : '';
  rowCountLabel.textContent = `${visibleLabel} • ${totalLabel}${filterSuffix}`;
}

function setDownloadAvailability(enabled: boolean) {
  if (!downloadButton) {
    return;
  }
  downloadButton.disabled = !enabled;
  downloadButton.title = enabled
    ? 'Download the currently visible rows as a CSV file'
    : 'Run a query to enable CSV exports';
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

function rememberQuery(sql: string) {
  const trimmed = sql.trim();
  if (!trimmed) {
    return;
  }

  queryHistory = [trimmed, ...queryHistory.filter((entry) => entry !== trimmed)].slice(0, MAX_HISTORY_ITEMS);
  renderQueryHistory();
}

function renderQueryHistory() {
  if (!historyList) {
    return;
  }

  if (queryHistory.length === 0) {
    historyList.innerHTML = '<div class="empty-state secondary">Your recent SQL will appear here.</div>';
    return;
  }

  historyList.innerHTML = '';
  const fragment = document.createDocumentFragment();

  queryHistory.forEach((entry) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'history-button';
    button.textContent = entry.length > 160 ? `${entry.slice(0, 157)}…` : entry;
    button.title = entry;
    button.addEventListener('click', () => {
      sqlInput.value = entry;
      runQuery(entry).catch(reportError);
    });
    fragment.appendChild(button);
  });

  historyList.appendChild(fragment);
}

function exportVisibleRowsToCSV() {
  if (!currentTableData) {
    return;
  }

  const visibleRows = getVisibleRows();
  if (visibleRows.length === 0) {
    updateStatus('There are no rows to export with the current filters.');
    return;
  }

  const headerLine = currentTableData.columns.map((column) => escapeCsvValue(column)).join(',');
  const dataLines = visibleRows.map((row) =>
    row.raw
      .map((value) => escapeCsvValue(formatValueForCsv(value)))
      .join(',')
  );

  const csvContent = [headerLine, ...dataLines].join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  link.download = `duckdb-results-${timestamp}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.setTimeout(() => URL.revokeObjectURL(url), 2000);
  updateStatus(`Exported ${integerFormatter.format(visibleRows.length)} row${visibleRows.length === 1 ? '' : 's'} as CSV.`);
  if (statusIcon) {
    statusIcon.textContent = '💾';
  }
}

function escapeCsvValue(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function formatValueForCsv(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (value instanceof Date) {
    return value.toISOString();
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

function updateInsights(data: TableData | null) {
  if (!insightCards || !schemaList) {
    return;
  }

  if (!data || data.rows.length === 0) {
    insightCards.innerHTML = '<div class="empty-state secondary">Run a query to unlock instant insights.</div>';
    schemaList.innerHTML = '<li class="schema-empty">Schema details will appear after your first query.</li>';
    return;
  }

  const summaries = computeColumnSummaries(data);
  insightCards.innerHTML = '';

  if (summaries.length === 0) {
    insightCards.innerHTML = '<div class="empty-state secondary">No columns detected for this result set.</div>';
  }

  const highlighted = summaries.slice(0, Math.min(6, summaries.length));
  highlighted.forEach((summary) => {
    insightCards.appendChild(buildInsightCard(summary));
  });

  schemaList.innerHTML = '';
  if (summaries.length === 0) {
    schemaList.innerHTML = '<li class="schema-empty">No schema metadata returned.</li>';
    return;
  }
  summaries.forEach((summary, index) => {
    const item = document.createElement('li');
    item.className = 'schema-item';
    const name = document.createElement('span');
    name.className = 'schema-name';
    name.textContent = summary.name;
    const type = document.createElement('span');
    type.className = 'schema-type';
    type.textContent = formatSchemaTypeLabel(data.types[index], summary);
    item.append(name, type);
    schemaList.appendChild(item);
  });
}

function computeColumnSummaries(data: TableData): ColumnSummary[] {
  const { columns, rows, types } = data;
  const summaries: ColumnSummary[] = columns.map((name, index) => ({
    name,
    declaredType: types[index] ?? '',
    observedType: 'mixed',
    nullCount: 0,
    nonNullCount: 0,
    uniqueValues: 0,
    sampleValues: [],
  }));

  const uniqueSets = columns.map(() => new Set<string>());
  const observedTypeSets = columns.map(() => new Set<ObservedType>());
  const numericStats = columns.map(() => ({ min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY, sum: 0, count: 0 }));
  const dateStats = columns.map(() => ({ min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY }));

  rows.forEach((row) => {
    row.raw.forEach((value, columnIndex) => {
      const summary = summaries[columnIndex];
      if (value === null || value === undefined || (typeof value === 'number' && !Number.isFinite(value))) {
        summary.nullCount += 1;
        return;
      }

      summary.nonNullCount += 1;

      const observedType = determineObservedType(value);
      observedTypeSets[columnIndex].add(observedType);

      const formattedValue = row.display[columnIndex] ?? formatCell(value);
      const uniqueSet = uniqueSets[columnIndex];
      if (uniqueSet.size < UNIQUE_SAMPLE_LIMIT) {
        uniqueSet.add(formattedValue);
      }
      if (summary.sampleValues.length < SAMPLE_VALUE_LIMIT && !summary.sampleValues.includes(formattedValue)) {
        summary.sampleValues.push(formattedValue);
      }

      if (observedType === 'number') {
        const numeric = numericStats[columnIndex];
        const numericValue = typeof value === 'number' ? value : Number(value);
        if (numericValue < numeric.min) {
          numeric.min = numericValue;
        }
        if (numericValue > numeric.max) {
          numeric.max = numericValue;
        }
        numeric.sum += numericValue;
        numeric.count += 1;
      } else if (observedType === 'date') {
        const date = value instanceof Date ? value : new Date(value);
        const timestamp = date.getTime();
        const stats = dateStats[columnIndex];
        if (timestamp < stats.min) {
          stats.min = timestamp;
        }
        if (timestamp > stats.max) {
          stats.max = timestamp;
        }
      }
    });
  });

  summaries.forEach((summary, index) => {
    summary.uniqueValues = uniqueSets[index].size;
    const observedSet = observedTypeSets[index];
    if (observedSet.size === 0) {
      summary.observedType = 'mixed';
    } else if (observedSet.size === 1) {
      summary.observedType = observedSet.values().next().value;
    } else if (observedSet.has('number') && observedSet.has('date') && observedSet.size === 2) {
      // Mixed numeric/date data should be considered mixed to avoid misleading stats
      summary.observedType = 'mixed';
    } else {
      summary.observedType = 'mixed';
    }

    const numeric = numericStats[index];
    if (numeric.count > 0 && summary.observedType === 'number') {
      summary.numeric = {
        min: numeric.min,
        max: numeric.max,
        avg: numeric.sum / numeric.count,
      };
    }

    const date = dateStats[index];
    if (date.min !== Number.POSITIVE_INFINITY && date.max !== Number.NEGATIVE_INFINITY && summary.observedType === 'date') {
      summary.dateRange = {
        min: new Date(date.min),
        max: new Date(date.max),
      };
    }
  });

  return summaries;
}

function determineObservedType(value: any): ObservedType {
  if (value instanceof Date) {
    return 'date';
  }
  const typeOf = typeof value;
  if (typeOf === 'number') {
    return 'number';
  }
  if (typeOf === 'string') {
    return 'string';
  }
  if (typeOf === 'boolean') {
    return 'boolean';
  }
  if (typeOf === 'object') {
    return 'object';
  }
  return 'mixed';
}

function formatSchemaTypeLabel(declaredType: string, summary: ColumnSummary): string {
  const friendly = friendlyTypeLabels[summary.observedType] ?? 'Unknown';
  if (declaredType && declaredType !== friendly) {
    return `${declaredType} • ${friendly}`;
  }
  return friendly;
}

function buildInsightCard(summary: ColumnSummary): HTMLElement {
  const card = document.createElement('div');
  card.className = 'insight-card';

  const header = document.createElement('div');
  header.className = 'insight-card-header';
  const name = document.createElement('h3');
  name.textContent = summary.name;
  const type = document.createElement('span');
  type.className = 'insight-pill';
  type.textContent = friendlyTypeLabels[summary.observedType] ?? 'Mixed';
  header.append(name, type);

  const body = document.createElement('div');
  body.className = 'insight-card-body';

  if (summary.numeric) {
    body.append(
      createInsightStat('Min', numberFormatter.format(summary.numeric.min)),
      createInsightStat('Avg', numberFormatter.format(summary.numeric.avg)),
      createInsightStat('Max', numberFormatter.format(summary.numeric.max)),
    );
  } else if (summary.dateRange) {
    body.append(
      createInsightStat('Earliest', dateTimeFormatter.format(summary.dateRange.min)),
      createInsightStat('Latest', dateTimeFormatter.format(summary.dateRange.max)),
    );
  } else {
    body.append(createInsightStat('Sample', formatSampleValues(summary.sampleValues)));
  }

  const footer = document.createElement('div');
  footer.className = 'insight-card-footer';
  footer.textContent = `${integerFormatter.format(summary.uniqueValues)} unique • ${integerFormatter.format(summary.nullCount)} null`;

  card.append(header, body, footer);
  return card;
}

function createInsightStat(label: string, value: string): HTMLElement {
  const wrapper = document.createElement('div');
  wrapper.className = 'insight-stat';
  const labelEl = document.createElement('span');
  labelEl.className = 'insight-stat-label';
  labelEl.textContent = label;
  const valueEl = document.createElement('span');
  valueEl.className = 'insight-stat-value';
  valueEl.textContent = value;
  wrapper.append(labelEl, valueEl);
  return wrapper;
}

function formatSampleValues(values: string[]): string {
  if (values.length === 0) {
    return 'All values are null';
  }
  return values.map((value) => (value.length > 32 ? `${value.slice(0, 29)}…` : value)).join(' • ');
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
  if (statusIcon) {
    statusIcon.textContent = '🛰️';
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
  if (statusIcon) {
    statusIcon.textContent = '🚨';
  }
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension to start the handshake
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
