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
const fileInfoLabel = document.getElementById('file-info');
const themeToggleButton = document.getElementById('theme-toggle') as HTMLButtonElement | null;
const downloadButton = document.getElementById('download-csv') as HTMLButtonElement | null;
const chartCanvas = document.getElementById('chart-canvas') as HTMLCanvasElement | null;
const chartEmptyState = document.getElementById('chart-empty');
const chartCaption = document.getElementById('chart-caption');
const schemaList = document.getElementById('schema-list');
const historyList = document.getElementById('history-list');
const metricRows = document.getElementById('metric-rows');
const metricRowsFootnote = document.getElementById('metric-rows-footnote');
const metricColumns = document.getElementById('metric-columns');
const metricColumnsFootnote = document.getElementById('metric-columns-footnote');
const metricNumeric = document.getElementById('metric-numeric');
const metricNumericFootnote = document.getElementById('metric-numeric-footnote');
const metricDuration = document.getElementById('metric-duration');
const metricDurationFootnote = document.getElementById('metric-duration-footnote');
const viewToggleButtons = Array.from(document.querySelectorAll<HTMLButtonElement>('[data-view-target]'));
const viewports = Array.from(document.querySelectorAll<HTMLElement>('.viewport'));

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
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
let lastVisibleRows: TableRow[] = [];
let currentSchema: { name: string; type: string; sample: string }[] = [];
let currentNumericSummary: { column: string; min: number; max: number; mean: number } | null = null;
let numericColumnCount = 0;
let lastQueryDuration = 0;
let cosmicMode = false;
let currentFileMeta: { name: string; size: number; loaderId: string } | null = null;
const queryHistory: { sql: string; timestamp: number; durationMs: number }[] = [];
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];

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

viewToggleButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const target = button.dataset.viewTarget;
    if (!target) {
      return;
    }
    setActiveView(target);
  });
});

if (themeToggleButton) {
  themeToggleButton.addEventListener('click', () => {
    cosmicMode = !cosmicMode;
    document.body.classList.toggle('theme-cosmic', cosmicMode);
    themeToggleButton.textContent = cosmicMode ? 'Return to Earth' : 'Activate Hyperdrive';
  });
}

if (downloadButton) {
  downloadButton.addEventListener('click', () => {
    try {
      exportCurrentView();
    } catch (err) {
      reportError(err);
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
  currentFileMeta = {
    name: fileName,
    size: fileBytes.length,
    loaderId: loader.id,
  };
  updateFileInfo();
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
  setActiveView('table-view');

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
    const start = performance.now();
    const result = await connection.query(sql);
    const duration = performance.now() - start;
    renderResults(result, duration);
    recordQueryHistory(sql, duration);

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

function renderResults(table: Table | null, durationMs: number) {
  if (!resultsContainer) {
    return;
  }

  lastQueryDuration = durationMs;
  currentNumericSummary = null;
  numericColumnCount = 0;
  currentSchema = [];
  resultsContainer.classList.add('active');
  resultsContainer.scrollTop = 0;

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    columnFilters = [];
    globalFilter = '';
    sortState = { columnIndex: -1, direction: null };
    lastVisibleRows = [];
    renderSchemaPanel();
    renderChartPreview();
    updateRowCount(0, 0);
    if (downloadButton) {
      downloadButton.disabled = true;
    }
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
  lastVisibleRows = rows;

  currentSchema = columns.map((column, index) => ({
    name: column,
    type: describeArrowType(table.schema.fields[index]),
    sample: rows.find((row) => row.display[index])?.display[index] ?? '',
  }));

  const numericIndices = columns
    .map((_, index) => index)
    .filter((index) => rows.some((row) => typeof row.raw[index] === 'number' && Number.isFinite(row.raw[index])));
  numericColumnCount = numericIndices.length;
  if (numericIndices.length > 0) {
    const selectedIndex = numericIndices[0];
    const values = rows
      .map((row) => row.raw[selectedIndex])
      .filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
    if (values.length > 0) {
      const min = Math.min(...values);
      const max = Math.max(...values);
      const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
      currentNumericSummary = {
        column: columns[selectedIndex],
        min,
        max,
        mean,
      };
    }
  }
  renderSchemaPanel();

  if (globalSearchInput) {
    globalSearchInput.value = '';
  }

  buildTableSkeleton(columns);
  applyTableState();

  if (downloadButton) {
    downloadButton.disabled = rows.length === 0;
  }
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

  lastVisibleRows = visibleRows;
  renderChartPreview();
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
  if (rowCountLabel) {
    if (total === 0) {
      rowCountLabel.textContent = 'No rows to display.';
    } else {
      rowCountLabel.textContent = `Showing ${visible.toLocaleString()} of ${total.toLocaleString()} rows`;
    }
  }
  updateMetrics(visible, total);
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

function renderChartPreview() {
  if (!chartCanvas || !chartEmptyState) {
    return;
  }

  if (!currentTableData || !currentTableData.columns.length) {
    chartCanvas.style.display = 'none';
    chartEmptyState.style.display = 'block';
    if (chartCaption) {
      chartCaption.textContent = '';
    }
    return;
  }

  const candidateRows = lastVisibleRows.length ? lastVisibleRows : currentTableData.rows;
  if (!candidateRows.length) {
    chartCanvas.style.display = 'none';
    chartEmptyState.style.display = 'block';
    if (chartCaption) {
      chartCaption.textContent = 'Filters removed all rows.';
    }
    return;
  }

  const numericIndex = currentTableData.columns.findIndex((_, index) =>
    candidateRows.some((row) => typeof row.raw[index] === 'number' && Number.isFinite(row.raw[index]))
  );

  if (numericIndex === -1) {
    chartCanvas.style.display = 'none';
    chartEmptyState.style.display = 'block';
    if (chartCaption) {
      chartCaption.textContent = 'No numeric columns available for charting yet.';
    }
    return;
  }

  const values = candidateRows
    .map((row) => row.raw[numericIndex])
    .filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
    .slice(0, 120);

  if (!values.length) {
    chartCanvas.style.display = 'none';
    chartEmptyState.style.display = 'block';
    if (chartCaption) {
      chartCaption.textContent = 'Numeric column detected, but all values are empty after filtering.';
    }
    return;
  }

  const container = chartCanvas.parentElement as HTMLElement | null;
  const bounds = container?.getBoundingClientRect();
  const fallbackWidth = bounds && bounds.width > 0 ? bounds.width : 420;
  const fallbackHeight = bounds && bounds.height > 0 ? bounds.height : 220;
  const dpr = window.devicePixelRatio || 1;
  chartCanvas.width = Math.floor(fallbackWidth * dpr);
  chartCanvas.height = Math.floor(fallbackHeight * dpr);

  const ctx = chartCanvas.getContext('2d');
  if (!ctx) {
    return;
  }

  ctx.save();
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, fallbackWidth, fallbackHeight);

  const padding = 24;
  const chartWidth = fallbackWidth - padding * 2;
  const chartHeight = fallbackHeight - padding * 2;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = Math.max(1, Math.floor(values.length / 50));
  const sampledValues = values.filter((_, index) => index % step === 0);
  const barWidth = Math.max(6, chartWidth / sampledValues.length - 4);

  const gradient = ctx.createLinearGradient(padding, padding, padding, padding + chartHeight);
  gradient.addColorStop(0, 'rgba(99, 102, 241, 0.8)');
  gradient.addColorStop(1, 'rgba(236, 72, 153, 0.6)');

  ctx.fillStyle = 'rgba(148, 163, 255, 0.12)';
  ctx.strokeStyle = 'rgba(148, 163, 255, 0.24)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  drawRoundedRect(ctx, padding, padding, chartWidth, chartHeight, 12);
  ctx.fill();
  ctx.stroke();

  ctx.fillStyle = gradient;
  sampledValues.forEach((value, index) => {
    const normalized = (value - min) / range;
    const barHeight = Math.max(6, normalized * (chartHeight - 12));
    const x = padding + index * (barWidth + 4);
    const y = padding + chartHeight - barHeight;
    ctx.beginPath();
    drawRoundedRect(ctx, x, y, barWidth, barHeight, 6);
    ctx.fill();
  });

  ctx.restore();

  chartCanvas.style.display = 'block';
  chartEmptyState.style.display = 'none';
  if (chartCaption) {
    const columnName = currentTableData.columns[numericIndex];
    chartCaption.textContent = `Previewing ${sampledValues.length.toLocaleString()} values from ${columnName}`;
  }
}

function renderSchemaPanel() {
  if (!schemaList) {
    return;
  }

  schemaList.innerHTML = '';
  if (!currentSchema.length) {
    const empty = document.createElement('li');
    empty.className = 'muted';
    empty.textContent = 'Run a query to explore schema details.';
    schemaList.appendChild(empty);
    return;
  }

  currentSchema.forEach((entry) => {
    const item = document.createElement('li');
    item.className = 'schema-entry';

    const headline = document.createElement('div');
    headline.className = 'schema-headline';
    const name = document.createElement('span');
    name.textContent = entry.name;
    const type = document.createElement('span');
    type.className = 'schema-type';
    type.textContent = entry.type;
    headline.append(name, type);

    item.appendChild(headline);

    if (entry.sample) {
      const sample = document.createElement('div');
      sample.className = 'schema-sample';
      sample.textContent = entry.sample;
      item.appendChild(sample);
    }

    schemaList.appendChild(item);
  });
}

function setActiveView(viewId: string) {
  viewToggleButtons.forEach((button) => {
    button.classList.toggle('active', button.dataset.viewTarget === viewId);
  });
  viewports.forEach((viewport) => {
    viewport.classList.toggle('active', viewport.id === viewId);
  });

  if (viewId === 'chart-view') {
    renderChartPreview();
  } else if (viewId === 'schema-view') {
    renderSchemaPanel();
  }
}

function exportCurrentView() {
  if (!currentTableData) {
    updateStatus('Nothing to export yet. Run a query first.');
    return;
  }

  const rowsToExport = lastVisibleRows.length ? lastVisibleRows : currentTableData.rows;
  if (!rowsToExport.length) {
    updateStatus('No rows available to export.');
    return;
  }

  const lines: string[] = [];
  lines.push(currentTableData.columns.map(csvEscape).join(','));

  rowsToExport.forEach((row) => {
    const line = row.raw
      .map((value) => csvEscape(formatCell(value)))
      .join(',');
    lines.push(line);
  });

  const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  const safeBase = (currentFileMeta?.name ?? 'duckdb-results').replace(/[^a-z0-9_-]+/gi, '_');
  anchor.href = url;
  anchor.download = `${safeBase || 'duckdb-results'}_view.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  updateStatus('CSV export generated from current view.');
}

function updateMetrics(visible: number, total: number) {
  setMetric(metricRows, total === 0 ? '0' : visible.toLocaleString());
  setMetric(
    metricRowsFootnote,
    total === 0 ? 'Waiting for data' : `of ${total.toLocaleString()} total rows`
  );

  const columnCount = currentTableData?.columns.length ?? 0;
  setMetric(metricColumns, columnCount ? columnCount.toString() : '0');
  setMetric(
    metricColumnsFootnote,
    columnCount ? `${columnCount === 1 ? 'column' : 'columns'} in result set` : 'Run a query to populate columns'
  );

  setMetric(metricNumeric, numericColumnCount ? numericColumnCount.toString() : '0');
  if (numericColumnCount && currentNumericSummary) {
    setMetric(
      metricNumericFootnote,
      `${currentNumericSummary.column}: ${formatNumber(currentNumericSummary.min)} → ${formatNumber(currentNumericSummary.max)}`
    );
  } else {
    setMetric(metricNumericFootnote, 'No numeric columns detected');
  }

  if (lastQueryDuration) {
    setMetric(metricDuration, `${lastQueryDuration.toFixed(1)} ms`);
    const fastest = queryHistory.reduce(
      (best, entry) => Math.min(best, entry.durationMs),
      lastQueryDuration
    );
    setMetric(metricDurationFootnote, `Fastest run ${fastest.toFixed(1)} ms`);
  } else {
    setMetric(metricDuration, '—');
    setMetric(metricDurationFootnote, 'Run a query to benchmark');
  }
}

function recordQueryHistory(sql: string, durationMs: number) {
  const text = sql.trim();
  if (!text) {
    return;
  }

  if (queryHistory.length && queryHistory[0].sql === text) {
    queryHistory[0] = { sql: text, timestamp: Date.now(), durationMs };
  } else {
    queryHistory.unshift({ sql: text, timestamp: Date.now(), durationMs });
    if (queryHistory.length > 12) {
      queryHistory.pop();
    }
  }

  renderHistory();
  updateMetrics(lastVisibleRows.length, currentTableData?.rows.length ?? 0);
}

function renderHistory() {
  if (!historyList) {
    return;
  }

  historyList.innerHTML = '';
  if (!queryHistory.length) {
    const placeholder = document.createElement('li');
    placeholder.className = 'muted';
    placeholder.textContent = 'No queries yet. Start exploring!';
    historyList.appendChild(placeholder);
    return;
  }

  queryHistory.forEach((entry) => {
    const item = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'history-item';

    const sqlPreview = document.createElement('span');
    sqlPreview.className = 'history-sql';
    sqlPreview.textContent = truncateSql(entry.sql);

    const meta = document.createElement('span');
    meta.className = 'history-meta';
    meta.textContent = `${timeAgo(entry.timestamp)} • ${entry.durationMs.toFixed(1)} ms`;

    button.append(sqlPreview, meta);
    button.addEventListener('click', () => {
      sqlInput.value = entry.sql;
      setActiveView('table-view');
      sqlInput.focus();
    });

    item.appendChild(button);
    historyList.appendChild(item);
  });
}

function updateFileInfo() {
  if (!fileInfoLabel) {
    return;
  }
  if (!currentFileMeta) {
    fileInfoLabel.textContent = 'Load a file to ignite the engines.';
    return;
  }

  const descriptor = [currentFileMeta.name];
  if (currentFileMeta.loaderId) {
    descriptor.push(currentFileMeta.loaderId.toUpperCase());
  }
  if (currentFileMeta.size) {
    descriptor.push(formatBytes(currentFileMeta.size));
  }
  fileInfoLabel.textContent = descriptor.join(' • ');
}

function describeArrowType(field: any): string {
  try {
    const type = (field as any)?.type;
    if (type && typeof type.toString === 'function') {
      return String(type.toString());
    }
    if (type?.TypeId !== undefined) {
      return String(type.TypeId);
    }
  } catch (err) {
    console.warn('[Webview] Unable to describe Arrow type', err);
  }
  return 'unknown';
}

function truncateSql(sql: string, max = 120): string {
  const compact = sql.replace(/\s+/g, ' ').trim();
  return compact.length > max ? `${compact.slice(0, max - 1)}…` : compact;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unitIndex = -1;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 100 || unitIndex === -1 ? 0 : 1)} ${units[Math.max(unitIndex, 0)]}`;
}

function csvEscape(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n') || value.includes('\r')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function setMetric(element: HTMLElement | null, text: string) {
  if (element) {
    element.textContent = text;
  }
}

function formatNumber(value: number): string {
  return Math.abs(value) >= 1000
    ? value.toLocaleString()
    : value.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function drawRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
) {
  const safeRadius = Math.max(0, Math.min(radius, width / 2, height / 2));
  if (typeof (ctx as any).roundRect === 'function') {
    (ctx as any).roundRect(x, y, width, height, safeRadius);
    return;
  }

  ctx.moveTo(x + safeRadius, y);
  ctx.lineTo(x + width - safeRadius, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + safeRadius);
  ctx.lineTo(x + width, y + height - safeRadius);
  ctx.quadraticCurveTo(x + width, y + height, x + width - safeRadius, y + height);
  ctx.lineTo(x + safeRadius, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - safeRadius);
  ctx.lineTo(x, y + safeRadius);
  ctx.quadraticCurveTo(x, y, x + safeRadius, y);
}

function timeAgo(timestamp: number): string {
  const delta = Date.now() - timestamp;
  if (delta < 5000) {
    return 'just now';
  }
  const seconds = Math.floor(delta / 1000);
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
  return `${days}d ago`;
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
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
