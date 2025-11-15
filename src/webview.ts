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
const themeToggleButton = document.getElementById('theme-toggle') as HTMLButtonElement | null;
const launchPartyButton = document.getElementById('launch-party') as HTMLButtonElement | null;
const galaxyTicker = document.getElementById('galaxy-ticker');

const telemetryElements = {
  visible: {
    value: document.getElementById('metric-visible-rows'),
    note: document.getElementById('note-visible-rows'),
    card: document.getElementById('card-visible-rows'),
  },
  total: {
    value: document.getElementById('metric-total-rows'),
    note: document.getElementById('note-total-rows'),
    card: document.getElementById('card-total-rows'),
  },
  columns: {
    value: document.getElementById('metric-columns'),
    note: document.getElementById('note-columns'),
    card: document.getElementById('card-columns'),
  },
  time: {
    value: document.getElementById('metric-query-time'),
    note: document.getElementById('note-query-time'),
    card: document.getElementById('card-query-time'),
  },
} as const;

type TelemetryKey = keyof typeof telemetryElements;

const telemetryFlashTimers = new Map<TelemetryKey, number>();

const cosmicTickerPhrases = [
  'Data dragons awakened. Feed them numbers.',
  'Hyperdrive calibrating. Hold on to your schemas.',
  'Quantum columns aligning across multiverses.',
  'Summoning parquet phoenix. Expect glitter.',
  'Warping CSVs into shimmering constellations.',
  'DuckDB oracles chanting in ANSI SQL.',
  'Slicing datasets thinner than photons.',
];

let lastQueryDurationMs = 0;
let lastQueryText = '';
let totalRowCount = 0;
let visibleRowCount = 0;

const cosmicCelebrations = [
  'Warp factor %speed engaged. %rows rows shimmer into focus!',
  'Quantum query stitched %rows rows in %speed. Reality recompiled.',
  'DuckDB lasers etched %rows rows at %speed. Pure spectacle.',
  'Data nebula parted: %rows rows swirling at %speed.',
  'Holographic grid stabilized: %rows rows, %speed to parse.',
];

const numberFormatter = new Intl.NumberFormat();

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

themeToggleButton?.addEventListener('click', () => {
  toggleHyperTheme();
});

launchPartyButton?.addEventListener('click', () => {
  triggerDataParty();
});

if (themeToggleButton) {
  themeToggleButton.textContent = 'Engage Aurora Mode';
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

  // Show status bar for "Running query..."
  updateStatus('Running query...');
  runButton.disabled = true;

  try {
    const start = performance.now();
    const result = await connection.query(sql);
    const duration = performance.now() - start;
    renderResults(result, duration, sql);
    celebrateQuerySuccess(result?.numRows ?? 0, duration);

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

function renderResults(table: Table | null, durationMs = 0, sql = '') {
  if (!resultsContainer) {
    return;
  }

  if (!table || table.numRows === 0) {
    resultsContainer.innerHTML = '<div class="empty-state">Query completed. No rows returned.</div>';
    currentTableData = null;
    tableBodyElement = null;
    totalRowCount = 0;
    visibleRowCount = 0;
    lastQueryDurationMs = durationMs;
    if (sql) {
      lastQueryText = sql;
    }
    updateRowCount(0, 0);
    updateTelemetry({ visible: 0, total: 0, columns: 0, duration: durationMs });
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

  totalRowCount = rows.length;
  lastQueryDurationMs = durationMs;
  visibleRowCount = rows.length;
  if (sql) {
    lastQueryText = sql;
  }
  updateTelemetry({
    visible: rows.length,
    total: rows.length,
    columns: columns.length,
    duration: durationMs,
  });
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
  rowCountLabel.textContent = `${formatNumber(visible)} visible / ${formatNumber(total)} total rows`; // display new info
  visibleRowCount = visible;
  totalRowCount = total;
  updateTelemetry({ visible, total });
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
  if (!Number.isFinite(value)) {
    return '0';
  }
  return numberFormatter.format(Math.round(value));
}

function formatDuration(durationMs: number): string {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return '0 ms';
  }
  if (durationMs < 1000) {
    const precision = durationMs < 100 ? 1 : 0;
    return `${durationMs.toFixed(precision)} ms`;
  }
  return `${(durationMs / 1000).toFixed(2)} s`;
}

function describeVisibleRows(visible: number, total: number): string {
  if (total === 0) {
    return 'Filters awaiting activation.';
  }
  if (visible === 0) {
    return 'Filters vaporized every row—check your constraints!';
  }
  if (visible === total) {
    return 'Full-spectrum nebula in view.';
  }
  return `${formatNumber(visible)} rays from ${formatNumber(total)} starfield.`;
}

function describeTotalRows(total: number): string {
  if (total === 0) {
    return 'No data on deck.';
  }
  if (total < 1000) {
    return 'Intimate constellation—perfect for detailed cartography.';
  }
  if (total < 100000) {
    return 'Swirling galaxy of insights ready for warp plotting.';
  }
  return 'Megacluster inbound. Prepare the antimatter aggregations.';
}

function describeColumns(columns: number): string {
  if (columns === 0) {
    return 'Awaiting schema transmission.';
  }
  if (columns < 5) {
    return 'Minimal orbit: sleek and lightning-fast.';
  }
  if (columns < 15) {
    return 'Balanced constellation—gravity just right.';
  }
  return 'Column supernova! Deploying holographic axes.';
}

function describeQueryTime(durationMs: number): string {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return lastQueryText ? `No jumps recorded · “${abbreviateQuery(lastQueryText)}”` : 'No jumps recorded.';
  }
  if (durationMs < 10) {
    return `Blink-and-you-miss-it warp${lastQueryText ? ` · “${abbreviateQuery(lastQueryText)}”` : ''}`;
  }
  if (durationMs < 40) {
    return `Hyperspace corridor silky smooth${lastQueryText ? ` · “${abbreviateQuery(lastQueryText)}”` : ''}`;
  }
  if (durationMs < 120) {
    return `Quantum thrusters humming elegantly${lastQueryText ? ` · “${abbreviateQuery(lastQueryText)}”` : ''}`;
  }
  if (durationMs < 600) {
    return `Sonic boom across the data ether${lastQueryText ? ` · “${abbreviateQuery(lastQueryText)}”` : ''}`;
  }
  return `Gravity wells detected—time for indices or snacks${lastQueryText ? ` · “${abbreviateQuery(lastQueryText)}”` : ''}`;
}

function abbreviateQuery(query: string): string {
  const compact = query.replace(/\s+/g, ' ').trim();
  if (!compact) {
    return '∅';
  }
  return compact.length > 48 ? `${compact.slice(0, 45)}…` : compact;
}

function updateTelemetry(update: { visible?: number; total?: number; columns?: number; duration?: number }) {
  if (update.visible !== undefined) {
    const element = telemetryElements.visible.value;
    if (element) {
      element.textContent = formatNumber(update.visible);
    }
    const note = telemetryElements.visible.note;
    if (note) {
      const total = update.total ?? totalRowCount;
      note.textContent = describeVisibleRows(update.visible, total);
    }
    flashTelemetryCard('visible');
  }

  if (update.total !== undefined) {
    const element = telemetryElements.total.value;
    if (element) {
      element.textContent = formatNumber(update.total);
    }
    const note = telemetryElements.total.note;
    if (note) {
      note.textContent = describeTotalRows(update.total);
    }
    flashTelemetryCard('total');
  }

  if (update.columns !== undefined) {
    const element = telemetryElements.columns.value;
    if (element) {
      element.textContent = formatNumber(update.columns);
    }
    const note = telemetryElements.columns.note;
    if (note) {
      note.textContent = describeColumns(update.columns);
    }
    flashTelemetryCard('columns');
  }

  if (update.duration !== undefined) {
    const element = telemetryElements.time.value;
    if (element) {
      element.textContent = formatDuration(update.duration);
    }
    const note = telemetryElements.time.note;
    if (note) {
      note.textContent = describeQueryTime(update.duration);
    }
    flashTelemetryCard('time');
  }
}

function flashTelemetryCard(key: TelemetryKey) {
  const card = telemetryElements[key].card as HTMLElement | null;
  if (!card) {
    return;
  }

  card.classList.add('active');

  const existing = telemetryFlashTimers.get(key);
  if (existing) {
    window.clearTimeout(existing);
  }
  const timeout = window.setTimeout(() => {
    card.classList.remove('active');
    telemetryFlashTimers.delete(key);
  }, 1600);
  telemetryFlashTimers.set(key, timeout);
}

function toggleHyperTheme() {
  const isAlt = document.body.classList.toggle('hyper-theme-alt');
  if (themeToggleButton) {
    themeToggleButton.textContent = isAlt ? 'Return to Cosmic Night' : 'Engage Aurora Mode';
  }
  broadcastTicker(isAlt ? 'Aurora mode engaged. Hues recalibrated for data stargazing.' : 'Hyperdrive default restored. Infinite midnight resumes.');
}

function triggerDataParty() {
  const sparks = 28;
  for (let i = 0; i < sparks; i++) {
    spawnSparkle();
  }
  broadcastTicker('Data party launched! Sparkles deployed across the schema horizon.');
}

function spawnSparkle() {
  const sparkle = document.createElement('div');
  sparkle.className = 'data-sparkle';
  const left = Math.random() * 100;
  sparkle.style.left = `${left}vw`;
  sparkle.style.top = `${-10 - Math.random() * 20}vh`;
  sparkle.style.animationDuration = `${1.2 + Math.random() * 0.8}s`;
  sparkle.style.animationDelay = `${Math.random() * 0.4}s`;
  document.body.appendChild(sparkle);
  window.setTimeout(() => {
    sparkle.remove();
  }, 2400);
}

function celebrateQuerySuccess(rows: number, durationMs: number) {
  if (rows <= 0) {
    broadcastTicker('Query executed at warp speed but returned the void. The void sparkles nonetheless.');
    return;
  }
  const template = randomFrom(cosmicCelebrations);
  if (!template) {
    return;
  }
  const formattedRows = formatNumber(rows);
  const formattedSpeed = formatDuration(durationMs);
  const message = template.replace('%rows', formattedRows).replace('%speed', formattedSpeed);
  broadcastTicker(message);
}

let lastTickerUpdate = Date.now();

function broadcastTicker(message: string) {
  if (!galaxyTicker) {
    return;
  }
  galaxyTicker.textContent = message;
  lastTickerUpdate = Date.now();
}

function randomFrom<T>(source: T[]): T | null {
  if (!source.length) {
    return null;
  }
  const index = Math.floor(Math.random() * source.length);
  return source[index] ?? null;
}

window.setInterval(() => {
  const now = Date.now();
  if (now - lastTickerUpdate < 22000) {
    return;
  }
  const headline = randomFrom(cosmicTickerPhrases);
  if (headline) {
    broadcastTicker(headline);
  }
}, 26000);

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
  broadcastTicker(message);
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
  broadcastTicker(`Error detected: ${message}`);
  console.error(`[Error] ${message}`, e);
}

// Send the 'ready' signal to the extension to start the handshake
updateStatus('Webview loaded. Sending "ready" to extension.');
vscode.postMessage({ command: 'ready' });
