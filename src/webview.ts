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
const globalSearchInput = document.getElementById('global-search') as HTMLInputElement;
const rowCountLabel = document.getElementById('row-count');
const insightsGrid = document.getElementById('insights-grid');
const historyList = document.getElementById('history-list');
const historyEmptyState = document.getElementById('history-empty');
const vizCanvas = document.getElementById('orbit-visualizer') as HTMLCanvasElement | null;
const vizTitle = document.getElementById('viz-title');
const vizSubtitle = document.getElementById('viz-subtitle');

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface QueryHistoryEntry {
  id: number;
  sql: string;
  normalizedSql: string;
  timestamp: number;
  durationMs: number;
  rowCount: number;
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
let queryHistory: QueryHistoryEntry[] = [];
let historySequence = 0;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

resizeOrbitCanvas();
window.addEventListener('resize', resizeOrbitCanvas);

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

  const trimmedSql = sql.trim();
  if (!trimmedSql) {
    updateStatus('Enter a SQL query to explore the cosmos.');
    return;
  }

  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const start = performance.now();
    const result = await connection.query(trimmedSql);
    const totalRows = renderResults(result);
    const durationMs = performance.now() - start;
    recordQuery(trimmedSql, totalRows, durationMs);

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

function renderResults(table: Table | null): number {
  if (!resultsContainer) {
    return 0;
  }

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    updateRowCount(0, 0);
    updateInsights();
    updateVisualization([]);
    return 0;
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
  updateInsights();
  return rows.length;
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
  updateVisualization(visibleRows);
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
  const columnCount = currentTableData?.columns.length ?? 0;
  const filterDescriptor = total === visible ? 'Full galaxy in view' : 'Filters engaged';
  rowCountLabel.textContent = `${formatNumber(visible)} / ${formatNumber(total)} rows • ${formatNumber(columnCount)} columns • ${filterDescriptor}`;
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

function recordQuery(sql: string, rowCount: number, durationMs: number) {
  if (!historyList) {
    return;
  }
  const normalizedSql = normalizeSql(sql);
  if (!normalizedSql) {
    return;
  }

  const entry: QueryHistoryEntry = {
    id: ++historySequence,
    sql,
    normalizedSql,
    timestamp: Date.now(),
    durationMs,
    rowCount,
  };

  if (queryHistory[0]?.normalizedSql === normalizedSql) {
    queryHistory[0] = entry;
  } else {
    queryHistory = [entry, ...queryHistory.filter((existing) => existing.normalizedSql !== normalizedSql)];
  }

  if (queryHistory.length > 20) {
    queryHistory = queryHistory.slice(0, 20);
  }

  renderQueryHistory();
}

function renderQueryHistory() {
  if (!historyList || !historyEmptyState) {
    return;
  }

  historyList.innerHTML = '';

  if (queryHistory.length === 0) {
    historyList.hidden = true;
    historyEmptyState.hidden = false;
    return;
  }

  historyList.hidden = false;
  historyEmptyState.hidden = true;

  queryHistory.forEach((entry) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'history-item';

    const sqlSpan = document.createElement('span');
    sqlSpan.className = 'history-sql';
    sqlSpan.textContent = entry.normalizedSql;

    const meta = document.createElement('div');
    meta.className = 'history-meta';

    const time = document.createElement('span');
    time.className = 'history-time';
    time.textContent = new Date(entry.timestamp).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });

    const count = document.createElement('span');
    count.className = 'history-count';
    count.textContent = `${formatNumber(entry.rowCount)} rows`;

    const duration = document.createElement('span');
    duration.textContent = formatDuration(entry.durationMs);

    meta.append(time, count, duration);
    button.append(sqlSpan, meta);
    button.addEventListener('click', () => {
      sqlInput.value = entry.sql;
      runQuery(entry.sql).catch(reportError);
    });

    historyList.appendChild(button);
  });
}

function updateInsights() {
  if (!insightsGrid) {
    return;
  }

  insightsGrid.innerHTML = '';

  if (!currentTableData || currentTableData.rows.length === 0) {
    return;
  }

  const totalRows = currentTableData.rows.length;
  const totalColumns = currentTableData.columns.length;

  let nullChampion: { column: string; ratio: number } = { column: '', ratio: 0 };
  let diversityChampion: { column: string; uniqueCount: number } = { column: '', uniqueCount: 0 };
  let numericChampion: { column: string; count: number; mean: number; min: number; max: number } = {
    column: '',
    count: 0,
    mean: 0,
    min: 0,
    max: 0,
  };

  currentTableData.columns.forEach((column, index) => {
    let nullCount = 0;
    let sum = 0;
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    let numericCount = 0;
    const uniqueSampler = new Set<string>();

    currentTableData!.rows.forEach((row) => {
      const value = row.raw[index];
      if (value === null || value === undefined || value === '') {
        nullCount += 1;
        return;
      }

      if (typeof value === 'number' && Number.isFinite(value)) {
        numericCount += 1;
        sum += value;
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
        return;
      }

      if (uniqueSampler.size < 250) {
        uniqueSampler.add(typeof value === 'string' ? value : JSON.stringify(value));
      }
    });

    if (totalRows > 0) {
      const ratio = nullCount / totalRows;
      if (ratio > nullChampion.ratio) {
        nullChampion = { column, ratio };
      }
    }

    if (uniqueSampler.size > diversityChampion.uniqueCount) {
      diversityChampion = { column, uniqueCount: uniqueSampler.size };
    }

    if (numericCount > numericChampion.count && numericCount > 0) {
      numericChampion = {
        column,
        count: numericCount,
        mean: sum / numericCount,
        min,
        max,
      };
    }
  });

  const cards: Array<{ label: string; value: string; meta: string }> = [
    {
      label: 'Row Horizon',
      value: formatNumber(totalRows),
      meta: `${formatNumber(totalColumns)} columns orbiting`,
    },
  ];

  if (numericChampion.count > 0) {
    cards.push({
      label: 'Brightest Metric',
      value: numericChampion.column,
      meta: `${formatNumber(numericChampion.count)} numeric values • μ ${formatNumber(numericChampion.mean, 2)} • ${formatNumber(numericChampion.min, 2)} → ${formatNumber(numericChampion.max, 2)}`,
    });
  }

  if (nullChampion.column) {
    cards.push({
      label: 'Null Nebula',
      value: nullChampion.column,
      meta: `${formatPercent(nullChampion.ratio)} of rows missing`,
    });
  }

  if (diversityChampion.column) {
    cards.push({
      label: 'Diversity Signal',
      value: diversityChampion.column,
      meta: `${formatNumber(diversityChampion.uniqueCount)} unique samples captured`,
    });
  }

  cards.slice(0, 4).forEach((card) => {
    const container = document.createElement('div');
    container.className = 'insight-card';

    const label = document.createElement('div');
    label.className = 'insight-label';
    label.textContent = card.label;

    const value = document.createElement('div');
    value.className = 'insight-value';
    value.textContent = card.value;

    const meta = document.createElement('div');
    meta.className = 'insight-meta';
    meta.textContent = card.meta;

    container.append(label, value, meta);
    insightsGrid.appendChild(container);
  });
}

function updateVisualization(rows: TableRow[]) {
  if (!vizCanvas || !vizTitle || !vizSubtitle) {
    return;
  }

  const ctx = vizCanvas.getContext('2d');
  if (!ctx) {
    return;
  }

  resizeOrbitCanvas();

  const displayWidth = vizCanvas.clientWidth || 320;
  const displayHeight = vizCanvas.clientHeight || 320;
  const dpr = window.devicePixelRatio || 1;

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, displayWidth, displayHeight);

  const centerX = displayWidth / 2;
  const centerY = displayHeight / 2;
  const radius = Math.min(centerX, centerY) - 12;

  ctx.save();
  ctx.translate(centerX, centerY);

  ctx.strokeStyle = 'rgba(115, 194, 255, 0.25)';
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(0, 0, radius, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(0, 0, radius * 0.65, 0, Math.PI * 2);
  ctx.stroke();

  const sourceRows = rows.length > 0 ? rows : currentTableData?.rows ?? [];
  const numericColumn = pickNumericColumn(sourceRows);

  if (!numericColumn) {
    ctx.restore();
    vizTitle.textContent = 'Awaiting numeric signals';
    vizSubtitle.textContent = 'Run a query with numeric columns to illuminate the orbit chart.';
    return;
  }

  const { values, name } = numericColumn;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 1);
  const mean = values.reduce((acc, value) => acc + value, 0) / values.length;

  const sampleSize = Math.min(values.length, 360);
  const step = values.length / sampleSize;

  for (let i = 0; i < sampleSize; i++) {
    const rawValue = values[Math.floor(i * step)];
    const normalized = (rawValue - min) / range;
    const angle = (i / sampleSize) * Math.PI * 2;
    const magnitude = radius * (0.25 + normalized * 0.7);
    const x = Math.cos(angle) * magnitude;
    const y = Math.sin(angle) * magnitude;

    ctx.strokeStyle = `hsla(${200 + normalized * 120}, 95%, ${65 + normalized * 20}%, 0.85)`;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(0, 0);
    ctx.lineTo(x, y);
    ctx.stroke();

    ctx.fillStyle = `hsla(${190 + normalized * 120}, 100%, 72%, 0.95)`;
    ctx.beginPath();
    ctx.arc(x, y, 2.8 + normalized * 2.4, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.restore();

  vizTitle.textContent = `Orbiting ${name}`;
  vizSubtitle.textContent = `${formatNumber(values.length)} values • μ ${formatNumber(mean, 2)} • ${formatNumber(min, 2)} → ${formatNumber(max, 2)}`;
}

function pickNumericColumn(rows: TableRow[]): { name: string; values: number[] } | null {
  if (!currentTableData) {
    return null;
  }

  let candidate: { index: number; name: string; values: number[] } | null = null;

  currentTableData.columns.forEach((column, columnIndex) => {
    const values: number[] = [];
    rows.forEach((row) => {
      const value = row.raw[columnIndex];
      if (typeof value === 'number' && Number.isFinite(value)) {
        values.push(value);
      }
    });

    if (values.length === 0) {
      return;
    }

    if (!candidate || values.length > candidate.values.length) {
      candidate = { index: columnIndex, name: column, values };
    }
  });

  if (!candidate) {
    return null;
  }

  return { name: candidate.name, values: candidate.values };
}

function resizeOrbitCanvas() {
  if (!vizCanvas) {
    return;
  }

  const displayWidth = vizCanvas.clientWidth || vizCanvas.width;
  const displayHeight = vizCanvas.clientHeight || vizCanvas.height;
  const dpr = window.devicePixelRatio || 1;

  const requiredWidth = Math.max(1, Math.floor(displayWidth * dpr));
  const requiredHeight = Math.max(1, Math.floor(displayHeight * dpr));

  if (vizCanvas.width !== requiredWidth || vizCanvas.height !== requiredHeight) {
    vizCanvas.width = requiredWidth;
    vizCanvas.height = requiredHeight;
  }
}

function formatNumber(value: number, fractionDigits = 0): string {
  if (!Number.isFinite(value)) {
    return '—';
  }
  return value.toLocaleString(undefined, {
    maximumFractionDigits: fractionDigits,
    minimumFractionDigits: fractionDigits > 0 ? Math.min(1, fractionDigits) : 0,
  });
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) {
    return '—';
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatDuration(ms: number): string {
  if (!Number.isFinite(ms)) {
    return '';
  }
  if (ms >= 1000) {
    return `${(ms / 1000).toFixed(2)}s`;
  }
  return `${Math.round(ms)}ms`;
}

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

// Send the 'ready' signal to the extension to start the handshake
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
