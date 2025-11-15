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
const fileLabel = document.getElementById('file-label');
const metricVisible = document.getElementById('metric-visible');
const metricTotal = document.getElementById('metric-total');
const metricColumns = document.getElementById('metric-columns');
const metricSchema = document.getElementById('metric-schema');
const metricSource = document.getElementById('metric-source');
const metricRun = document.getElementById('metric-run');
const downloadCsvButton = document.getElementById('download-csv') as HTMLButtonElement | null;
const resetViewButton = document.getElementById('reset-view') as HTMLButtonElement | null;
const insightsList = document.getElementById('insights-list');
const historyContainer = document.getElementById('history-chips');
const chartXSelect = document.getElementById('chart-x') as HTMLSelectElement | null;
const chartYSelect = document.getElementById('chart-y') as HTMLSelectElement | null;
const chartModeSelect = document.getElementById('chart-mode') as HTMLSelectElement | null;
const chartCanvas = document.getElementById('chart-canvas') as HTMLCanvasElement | null;
const chartEmptyState = document.getElementById('chart-empty');

type SortDirection = 'asc' | 'desc' | null;
type ChartMode = 'avg' | 'sum' | 'min' | 'max';

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

interface ColumnProfile {
  name: string;
  index: number;
  sampleSize: number;
  uniqueCount: number;
  emptyCount: number;
  numericCount: number;
  numericMin: number;
  numericMax: number;
  numericTotal: number;
  stringSamples: string[];
  booleanTrue: number;
  booleanFalse: number;
  isNumeric: boolean;
}

interface ChartAggregate {
  label: string;
  sum: number;
  count: number;
  avg: number;
  min: number;
  max: number;
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
let currentFileName: string | null = null;
let currentLoaderId: string | null = null;
let lastVisibleRows: TableRow[] = [];
let queryHistory: string[] = [];
let runCount = 0;
let chartState: { xColumn: string | null; yColumn: string | null; mode: ChartMode } = {
  xColumn: null,
  yColumn: null,
  mode: 'avg',
};
let baseColumnProfiles: ColumnProfile[] = [];

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

if (downloadCsvButton) {
  downloadCsvButton.addEventListener('click', () => {
    try {
      downloadVisibleRows();
    } catch (error) {
      reportError(error);
    }
  });
}

if (resetViewButton) {
  resetViewButton.addEventListener('click', () => {
    resetViewState();
  });
}

if (chartXSelect) {
  chartXSelect.addEventListener('change', () => {
    chartState.xColumn = chartXSelect.value || null;
    renderOrbitChart();
  });
}
if (chartYSelect) {
  chartYSelect.addEventListener('change', () => {
    chartState.yColumn = chartYSelect.value || null;
    renderOrbitChart();
  });
}
if (chartModeSelect) {
  chartModeSelect.addEventListener('change', () => {
    const value = chartModeSelect.value as ChartMode;
    chartState.mode = value;
    renderOrbitChart();
  });
}

window.addEventListener('resize', () => {
  // Re-render the chart when layout changes so the canvas stays crisp.
  renderOrbitChart();
});

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
  currentFileName = fileName;
  currentLoaderId = loader.id;
  updateFileLabel(fileName);
  updateSourceMetric(loader.id);
  updateStatus(`Preparing ${loader.id.toUpperCase()} data for ${fileName}…`);
  const loadResult = await loader.load(fileName, fileBytes, {
    db,
    connection,
    updateStatus,
  });

  const defaultQuery = buildDefaultQuery(loadResult.columns, loadResult.relationIdentifier);
  sqlInput.value = defaultQuery;
  sqlInput.placeholder = `Example: ${defaultQuery}`;
  updateSchemaMetric(loadResult.columns);

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
    addQueryToHistory(sql);
    runCount += 1;
    updateRunMetric();

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
    lastVisibleRows = [];
    updateInsights([]);
    clearOrbitControls();
    renderOrbitChart();
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

  updateSchemaMetric(columns);
  buildTableSkeleton(columns);
  populateOrbitControls();
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
  updateRowCount(visibleRows.length, currentTableData.rows.length);
  refreshSortIndicators();
  updateInsights(visibleRows);
  renderOrbitChart();
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
      rowCountLabel.textContent = 'No rows to display yet.';
    } else {
      rowCountLabel.textContent = `Showing ${formatNumber(visible)} of ${formatNumber(total)} rows`;
    }
  }
  if (metricVisible) {
    metricVisible.textContent = formatNumber(visible);
  }
  if (metricTotal) {
    metricTotal.textContent = `of ${formatNumber(total)} total rows`;
  }
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

function formatNumber(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

function updateFileLabel(fileName: string | null) {
  if (!fileLabel) {
    return;
  }
  if (!fileName) {
    fileLabel.textContent = 'Load a file or run a query to begin the adventure.';
    return;
  }
  fileLabel.textContent = `Currently orbiting: ${fileName}`;
}

function updateSourceMetric(loaderId: string | null) {
  if (!metricSource) {
    return;
  }
  if (!loaderId) {
    metricSource.textContent = '—';
    return;
  }
  metricSource.textContent = loaderId.toUpperCase();
}

function updateSchemaMetric(columns: string[]) {
  if (!metricColumns || !metricSchema) {
    return;
  }
  metricColumns.textContent = formatNumber(columns.length);
  if (columns.length === 0) {
    metricSchema.textContent = 'Awaiting schema…';
  } else {
    const preview = columns.slice(0, 3).join(', ');
    metricSchema.textContent = columns.length > 3 ? `${preview}, …` : preview;
  }
}

function updateRunMetric() {
  if (!metricRun) {
    return;
  }
  if (runCount <= 0) {
    metricRun.textContent = 'No queries yet';
  } else {
    const timestamp = new Date();
    metricRun.textContent = `Run #${runCount} @ ${timestamp.toLocaleTimeString()}`;
  }
}

function addQueryToHistory(sql: string) {
  const normalized = sql.trim();
  if (!normalized) {
    return;
  }
  queryHistory = [normalized, ...queryHistory.filter((entry) => entry !== normalized)];
  if (queryHistory.length > 10) {
    queryHistory = queryHistory.slice(0, 10);
  }
  renderQueryHistory();
}

function renderQueryHistory() {
  if (!historyContainer) {
    return;
  }
  historyContainer.innerHTML = '';
  if (queryHistory.length === 0) {
    historyContainer.classList.add('history-empty');
    historyContainer.textContent = 'No queries yet. Launch one!';
    return;
  }
  historyContainer.classList.remove('history-empty');
  const fragment = document.createDocumentFragment();
  queryHistory.forEach((query) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'history-chip';
    button.textContent = query.length > 120 ? `${query.slice(0, 117)}…` : query;
    button.title = query;
    button.addEventListener('click', () => {
      sqlInput.value = query;
      runQuery(query).catch(reportError);
    });
    fragment.appendChild(button);
  });
  historyContainer.appendChild(fragment);
}

function resetViewState() {
  if (!currentTableData) {
    updateStatus('No data yet. Load a file to explore.');
    return;
  }
  columnFilters = currentTableData.columns.map(() => '');
  globalFilter = '';
  sortState = { columnIndex: -1, direction: null };
  if (globalSearchInput) {
    globalSearchInput.value = '';
  }
  const filterInputs = resultsContainer?.querySelectorAll<HTMLInputElement>('.filter-row input');
  filterInputs?.forEach((input) => {
    input.value = '';
  });
  applyTableState();
  updateStatus('View reset. Showing cosmic default ordering.');
}

function downloadVisibleRows() {
  if (!currentTableData) {
    updateStatus('No data to export yet. Run a query first.');
    return;
  }
  const rows = lastVisibleRows.length > 0 ? lastVisibleRows : currentTableData.rows;
  if (rows.length === 0) {
    updateStatus('There are no rows in the current preview to download.');
    return;
  }
  const header = currentTableData.columns.map(escapeCsvValue).join(',');
  const body = rows
    .map((row) => row.display.map(escapeCsvValue).join(','))
    .join('\n');
  const csvContent = `${header}\n${body}`;
  const blob = new Blob([csvContent], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  const safeName = (currentFileName ?? 'duckdb_preview').replace(/[^a-z0-9-_]/gi, '_');
  link.download = `${safeName}_preview.csv`;
  link.href = url;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  updateStatus(`Downloaded ${formatNumber(rows.length)} rows as CSV preview.`);
}

function escapeCsvValue(value: string): string {
  const safeValue = value ?? '';
  if (/[",\n]/.test(safeValue)) {
    return `"${safeValue.replace(/"/g, '""')}"`;
  }
  return safeValue;
}

function populateOrbitControls() {
  if (!chartXSelect || !chartYSelect) {
    return;
  }
  if (!currentTableData) {
    clearOrbitControls();
    return;
  }
  baseColumnProfiles = computeColumnProfiles(currentTableData.rows, currentTableData.columns);
  const categorical = baseColumnProfiles.filter((profile) => !profile.isNumeric);
  const numeric = baseColumnProfiles.filter((profile) => profile.isNumeric);

  setSelectOptions(chartXSelect, categorical.length > 0 ? categorical : baseColumnProfiles);
  setSelectOptions(chartYSelect, numeric.length > 0 ? numeric : baseColumnProfiles);

  if (chartState.xColumn && !baseColumnProfiles.some((profile) => profile.name === chartState.xColumn)) {
    chartState.xColumn = null;
  }
  if (chartState.yColumn && !baseColumnProfiles.some((profile) => profile.name === chartState.yColumn)) {
    chartState.yColumn = null;
  }
  if (!chartState.xColumn && chartXSelect.options.length > 0) {
    chartState.xColumn = chartXSelect.options[0].value;
  }
  if (!chartState.yColumn && chartYSelect.options.length > 0) {
    chartState.yColumn = chartYSelect.options[0].value;
  }

  if (chartState.xColumn) {
    chartXSelect.value = chartState.xColumn;
  }
  if (chartState.yColumn) {
    chartYSelect.value = chartState.yColumn;
  }

  renderOrbitChart();
}

function setSelectOptions(select: HTMLSelectElement, profiles: ColumnProfile[]) {
  select.innerHTML = '';
  const fragment = document.createDocumentFragment();
  profiles.forEach((profile) => {
    const option = document.createElement('option');
    option.value = profile.name;
    option.textContent = profile.name;
    fragment.appendChild(option);
  });
  if (profiles.length === 0) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No columns';
    fragment.appendChild(option);
  }
  select.appendChild(fragment);
  select.disabled = profiles.length === 0;
}

function clearOrbitControls() {
  if (chartXSelect) {
    chartXSelect.innerHTML = '';
    chartXSelect.disabled = true;
  }
  if (chartYSelect) {
    chartYSelect.innerHTML = '';
    chartYSelect.disabled = true;
  }
  chartState.xColumn = null;
  chartState.yColumn = null;
  baseColumnProfiles = [];
  if (chartCanvas) {
    const ctx = chartCanvas.getContext('2d');
    if (ctx) {
      ctx.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    }
  }
  if (chartEmptyState) {
    chartEmptyState.hidden = false;
  }
}

function renderOrbitChart() {
  if (!chartCanvas || !chartEmptyState) {
    return;
  }
  if (!currentTableData || !chartState.xColumn || !chartState.yColumn) {
    chartEmptyState.hidden = false;
    const ctx = chartCanvas.getContext('2d');
    ctx?.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    return;
  }

  const xIndex = currentTableData.columns.indexOf(chartState.xColumn);
  const yIndex = currentTableData.columns.indexOf(chartState.yColumn);
  if (xIndex === -1 || yIndex === -1) {
    chartEmptyState.hidden = false;
    return;
  }

  const aggregated = collectChartData(xIndex, yIndex);
  if (aggregated.length === 0) {
    chartEmptyState.hidden = false;
    const ctx = chartCanvas.getContext('2d');
    ctx?.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    return;
  }

  const processed = aggregated
    .map((entry) => {
      let value = entry.avg;
      switch (chartState.mode) {
        case 'sum':
          value = entry.sum;
          break;
        case 'min':
          value = entry.min;
          break;
        case 'max':
          value = entry.max;
          break;
        default:
          value = entry.avg;
      }
      return { ...entry, value };
    })
    .sort((a, b) => b.value - a.value)
    .slice(0, 12);

  if (processed.length === 0) {
    chartEmptyState.hidden = false;
    const ctx = chartCanvas.getContext('2d');
    ctx?.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    return;
  }

  chartEmptyState.hidden = true;
  const ctx = chartCanvas.getContext('2d');
  if (!ctx) {
    return;
  }

  const ratio = window.devicePixelRatio || 1;
  const width = chartCanvas.clientWidth * ratio;
  const height = chartCanvas.clientHeight * ratio;
  if (chartCanvas.width !== width || chartCanvas.height !== height) {
    chartCanvas.width = width;
    chartCanvas.height = height;
  }

  ctx.save();
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, chartCanvas.clientWidth, chartCanvas.clientHeight);

  const padding = 40;
  const availableWidth = Math.max(10, chartCanvas.clientWidth - padding * 2);
  const availableHeight = Math.max(10, chartCanvas.clientHeight - padding * 2);

  const values = processed.map((entry) => entry.value);
  const minValue = Math.min(0, ...values);
  const maxValue = Math.max(0, ...values);
  const range = maxValue - minValue || 1;
  const zeroLine = padding + ((maxValue - 0) / range) * availableHeight;

  const gradient = ctx.createLinearGradient(0, 0, chartCanvas.clientWidth, chartCanvas.clientHeight);
  gradient.addColorStop(0, 'rgba(99, 102, 241, 0.35)');
  gradient.addColorStop(1, 'rgba(236, 72, 153, 0.35)');
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, chartCanvas.clientWidth, chartCanvas.clientHeight);

  ctx.strokeStyle = 'rgba(255, 255, 255, 0.18)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, padding + availableHeight);
  ctx.lineTo(padding + availableWidth, padding + availableHeight);
  ctx.stroke();

  ctx.strokeStyle = 'rgba(255, 255, 255, 0.1)';
  ctx.beginPath();
  ctx.moveTo(padding, zeroLine);
  ctx.lineTo(padding + availableWidth, zeroLine);
  ctx.stroke();

  const slotWidth = availableWidth / processed.length;
  const barWidth = Math.min(60, slotWidth * 0.7);

  processed.forEach((entry, index) => {
    const x = padding + index * slotWidth + (slotWidth - barWidth) / 2;
    const valueY = padding + ((maxValue - entry.value) / range) * availableHeight;
    const barTop = Math.min(valueY, zeroLine);
    const barBottom = Math.max(valueY, zeroLine);
    const barHeight = Math.max(4, barBottom - barTop);

    const hue = 220 + (index / Math.max(1, processed.length - 1)) * 120;
    const baseColor = `hsla(${hue}, 85%, 62%, 0.85)`;
    ctx.fillStyle = baseColor;
    drawRoundedRect(ctx, x, barTop, barWidth, barHeight, 8);
    ctx.fill();

    ctx.fillStyle = 'rgba(255, 255, 255, 0.85)';
    ctx.font = '12px "Segoe UI", sans-serif';
    ctx.textAlign = 'center';
    const valueLabelY = entry.value >= 0 ? barTop - 6 : barBottom + 14;
    ctx.fillText(formatNumber(entry.value), x + barWidth / 2, valueLabelY);

    ctx.save();
    ctx.translate(x + barWidth / 2, padding + availableHeight + 16);
    ctx.rotate(-Math.PI / 8);
    ctx.fillStyle = 'rgba(255, 255, 255, 0.8)';
    ctx.fillText(truncateLabel(entry.label, 18), 0, 0);
    ctx.restore();
  });

  ctx.restore();
}

function collectChartData(xIndex: number, yIndex: number): ChartAggregate[] {
  if (!currentTableData) {
    return [];
  }
  const rows = (lastVisibleRows.length > 0 ? lastVisibleRows : currentTableData.rows).slice(0, 1500);
  const buckets = new Map<string, { sum: number; count: number; min: number; max: number }>();
  rows.forEach((row) => {
    const labelRaw = row.display[xIndex] ?? '';
    const label = labelRaw.trim() === '' ? '(blank)' : labelRaw;
    const rawValue = row.raw[yIndex];
    const numericValue = typeof rawValue === 'number' ? rawValue : Number(rawValue);
    if (!Number.isFinite(numericValue)) {
      return;
    }
    const bucket = buckets.get(label) ?? {
      sum: 0,
      count: 0,
      min: Number.POSITIVE_INFINITY,
      max: Number.NEGATIVE_INFINITY,
    };
    bucket.sum += numericValue;
    bucket.count += 1;
    bucket.min = Math.min(bucket.min, numericValue);
    bucket.max = Math.max(bucket.max, numericValue);
    buckets.set(label, bucket);
  });
  return Array.from(buckets.entries())
    .map(([label, bucket]) => ({
      label,
      sum: bucket.sum,
      count: bucket.count,
      avg: bucket.count > 0 ? bucket.sum / bucket.count : 0,
      min: bucket.min === Number.POSITIVE_INFINITY ? 0 : bucket.min,
      max: bucket.max === Number.NEGATIVE_INFINITY ? 0 : bucket.max,
    }))
    .filter((entry) => entry.count > 0);
}

function updateInsights(visibleRows: TableRow[]) {
  if (!insightsList) {
    return;
  }
  insightsList.innerHTML = '';
  if (!currentTableData) {
    insightsList.appendChild(createInsightText('🦆 Load a file to generate insights.'));
    return;
  }

  if (visibleRows.length === 0 && currentTableData.rows.length > 0) {
    insightsList.appendChild(
      createInsightText('🪐 Filters active: no rows match the current constellation. Try resetting the view.')
    );
  }

  const rowsForAnalysis = visibleRows.length > 0 ? visibleRows : currentTableData.rows;
  if (rowsForAnalysis.length === 0) {
    insightsList.appendChild(createInsightText('🛰️ Awaiting data. Run a query to illuminate the grid.'));
    return;
  }

  const profiles = computeColumnProfiles(rowsForAnalysis, currentTableData.columns);
  if (profiles.length === 0) {
    insightsList.appendChild(createInsightText('🚀 Data ready, but we could not extract column profiles yet.'));
    return;
  }

  insightsList.appendChild(
    createInsightText(
      `🧮 Galactic preview: ${formatNumber(rowsForAnalysis.length)} rows × ${formatNumber(
        currentTableData.columns.length
      )} columns in view.`
    )
  );

  const mostUnique = profiles.reduce((prev, curr) => (curr.uniqueCount > prev.uniqueCount ? curr : prev), profiles[0]);
  if (mostUnique) {
    insightsList.appendChild(
      createInsightText(
        `🎯 Most diverse column “${mostUnique.name}” exposes ${formatNumber(mostUnique.uniqueCount)} unique values.`
      )
    );
  }

  const numericProfiles = profiles.filter((profile) => profile.isNumeric && profile.numericCount > 0);
  if (numericProfiles.length > 0) {
    const widest = numericProfiles.reduce((prev, curr) => {
      const prevSpread = prev.numericMax - prev.numericMin;
      const currSpread = curr.numericMax - curr.numericMin;
      return currSpread > prevSpread ? curr : prev;
    }, numericProfiles[0]);
    const average = widest.numericCount > 0 ? widest.numericTotal / widest.numericCount : 0;
    insightsList.appendChild(
      createInsightText(
        `📈 Numeric rocket “${widest.name}” ranges ${formatNumber(widest.numericMin)} → ${formatNumber(
          widest.numericMax
        )} (avg ${formatNumber(average)}).`
      )
    );
  }

  const nullHeavy = profiles.reduce((prev, curr) => (curr.emptyCount > prev.emptyCount ? curr : prev), profiles[0]);
  if (nullHeavy && nullHeavy.emptyCount > 0) {
    const ratio = rowsForAnalysis.length > 0 ? (nullHeavy.emptyCount / rowsForAnalysis.length) * 100 : 0;
    insightsList.appendChild(
      createInsightText(
        `🪄 Missing data alert: “${nullHeavy.name}” has ${formatNumber(nullHeavy.emptyCount)} blanks (~${ratio.toFixed(
          1
        )}%).`
      )
    );
  }
}

function computeColumnProfiles(rows: TableRow[], columns: string[]): ColumnProfile[] {
  const limit = Math.min(rows.length, 1500);
  const uniqueTrackers = columns.map(() => new Set<string>());
  const stringSamples = columns.map(() => new Set<string>());
  const emptyCounts = columns.map(() => 0);
  const numericCounts = columns.map(() => 0);
  const numericTotals = columns.map(() => 0);
  const numericMins = columns.map(() => Number.POSITIVE_INFINITY);
  const numericMaxs = columns.map(() => Number.NEGATIVE_INFINITY);
  const booleanTrue = columns.map(() => 0);
  const booleanFalse = columns.map(() => 0);

  for (let rowIndex = 0; rowIndex < limit; rowIndex++) {
    const row = rows[rowIndex];
    columns.forEach((_, columnIndex) => {
      const displayValue = row.display[columnIndex] ?? '';
      const rawValue = row.raw[columnIndex];

      if (displayValue === '' || displayValue === null) {
        emptyCounts[columnIndex] += 1;
      }
      if (uniqueTrackers[columnIndex].size < 512) {
        uniqueTrackers[columnIndex].add(displayValue);
      }
      if (displayValue && stringSamples[columnIndex].size < 4) {
        stringSamples[columnIndex].add(displayValue);
      }

      if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
        numericCounts[columnIndex] += 1;
        numericTotals[columnIndex] += rawValue;
        numericMins[columnIndex] = Math.min(numericMins[columnIndex], rawValue);
        numericMaxs[columnIndex] = Math.max(numericMaxs[columnIndex], rawValue);
        return;
      }
      if (typeof rawValue === 'boolean') {
        if (rawValue) {
          booleanTrue[columnIndex] += 1;
        } else {
          booleanFalse[columnIndex] += 1;
        }
        return;
      }
      if (rawValue instanceof Date) {
        const numericTime = rawValue.getTime();
        numericCounts[columnIndex] += 1;
        numericTotals[columnIndex] += numericTime;
        numericMins[columnIndex] = Math.min(numericMins[columnIndex], numericTime);
        numericMaxs[columnIndex] = Math.max(numericMaxs[columnIndex], numericTime);
      }
    });
  }

  return columns.map((name, index) => {
    const sampleSize = limit;
    const numericCount = numericCounts[index];
    const isNumeric = numericCount >= Math.max(3, Math.floor(sampleSize * 0.5));
    return {
      name,
      index,
      sampleSize,
      uniqueCount: uniqueTrackers[index].size,
      emptyCount: emptyCounts[index],
      numericCount,
      numericMin: numericCount > 0 && numericMins[index] !== Number.POSITIVE_INFINITY ? numericMins[index] : 0,
      numericMax: numericCount > 0 && numericMaxs[index] !== Number.NEGATIVE_INFINITY ? numericMaxs[index] : 0,
      numericTotal: numericTotals[index],
      stringSamples: Array.from(stringSamples[index]),
      booleanTrue: booleanTrue[index],
      booleanFalse: booleanFalse[index],
      isNumeric,
    };
  });
}

function createInsightText(text: string) {
  const item = document.createElement('li');
  item.className = 'insight-card';
  item.textContent = text;
  return item;
}

function truncateLabel(label: string, maxLength: number) {
  if (label.length <= maxLength) {
    return label;
  }
  return `${label.slice(0, maxLength - 1)}…`;
}

function drawRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
) {
  const r = Math.max(0, Math.min(radius, Math.abs(width) / 2, Math.abs(height) / 2));
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
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
updateFileLabel(null);
updateSourceMetric(null);
updateSchemaMetric([]);
updateRunMetric();
updateRowCount(0, 0);
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
