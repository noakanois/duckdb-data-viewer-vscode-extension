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
const snapshotButton = document.getElementById('snapshot-result') as HTMLButtonElement | null;
const queryHistoryList = document.getElementById('query-history-list');
const snapshotList = document.getElementById('snapshot-list');
const hyperOutput = document.getElementById('hyper-output');
const hyperActionButtons = Array.from(document.querySelectorAll<HTMLButtonElement>('.hyper-action'));

type SortDirection = 'asc' | 'desc' | null;

interface TableRow {
  raw: any[];
  display: string[];
}

interface TableData {
  columns: string[];
  rows: TableRow[];
}

type ColumnCategory = 'numeric' | 'temporal' | 'boolean' | 'other';

interface ColumnMetadata {
  name: string;
  type: string;
  category: ColumnCategory;
}

interface ActiveRelation {
  relationName: string;
  relationIdentifier: string;
  columns: string[];
  metadata: ColumnMetadata[];
}

interface QueryHistoryEntry {
  id: string;
  sql: string;
  timestamp: number;
}

interface SnapshotEntry {
  id: string;
  query: string;
  timestamp: number;
  rowCount: number;
  sample: string | null;
  columns: string[];
}

interface ColumnProfileRow {
  column_position: number;
  column_name: string;
  data_type: string;
  total_rows: number;
  null_count: number;
  distinct_count: number;
  dominant_value: any;
  min_value: any;
  max_value: any;
  average_value: number | null;
  truth_ratio: number | null;
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
let activeRelation: ActiveRelation | null = null;
let queryHistory: QueryHistoryEntry[] = [];
let snapshots: SnapshotEntry[] = [];
let lastExecutedQuery = '';
let cachedProfile: ColumnProfileRow[] | null = null;
const DATA_LOADERS: DataLoader[] = [arrowLoader, parquetLoader, csvLoader];
const MAX_HISTORY_ENTRIES = 30;
const MAX_SNAPSHOTS = 12;

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

if (snapshotButton) {
  snapshotButton.addEventListener('click', () => {
    try {
      captureSnapshot();
    } catch (err) {
      reportError(err);
    }
  });
}

hyperActionButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const action = button.dataset.action ?? '';
    handleHyperAction(action, button).catch(reportError);
  });
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

  const metadata = await introspectColumnMetadata(loadResult.relationName, loadResult.columns);
  activeRelation = {
    relationName: loadResult.relationName,
    relationIdentifier: loadResult.relationIdentifier,
    columns: loadResult.columns,
    metadata,
  };
  cachedProfile = null;
  queryHistory = [];
  snapshots = [];
  lastExecutedQuery = '';
  updateQueryHistoryList();
  updateSnapshotList();
  renderHyperStatus(`Hyper actions primed for ${loadResult.relationName}.`, 'Fire a button above to unleash automated analysis.');

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
    lastExecutedQuery = sql;
    recordQueryHistory(sql);

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
  rowCountLabel.textContent = '';
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

function categorizeColumnType(type: string): ColumnCategory {
  const normalized = type?.toLowerCase?.() ?? '';
  if (/int|decimal|double|float|real|numeric|hugeint|smallint|tinyint|ubigint|uint|bigint/.test(normalized)) {
    return 'numeric';
  }
  if (/timestamp|date|time|interval/.test(normalized)) {
    return 'temporal';
  }
  if (/bool/.test(normalized)) {
    return 'boolean';
  }
  return 'other';
}

function escapeSqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

async function introspectColumnMetadata(relationName: string, fallbackColumns: string[]): Promise<ColumnMetadata[]> {
  if (!connection) {
    return fallbackColumns.map((name) => ({ name, type: 'UNKNOWN', category: 'other' }));
  }
  try {
    const escaped = relationName.replace(/'/g, "''");
    const table = await connection.query(`PRAGMA table_info('${escaped}');`);
    const rows = table.toArray() as any[];
    const metadata = rows
      .map((row) => {
        const name: string | undefined = typeof row.name === 'string' ? row.name : typeof row.column_name === 'string' ? row.column_name : undefined;
        if (!name || name.length === 0) {
          return null;
        }
        const type: string = typeof row.type === 'string' ? row.type : typeof row.data_type === 'string' ? row.data_type : 'UNKNOWN';
        return { name, type };
      })
      .filter((entry): entry is { name: string; type: string } => !!entry);

    if (metadata.length === 0) {
      return fallbackColumns.map((name) => ({ name, type: 'UNKNOWN', category: 'other' }));
    }

    return metadata.map(({ name, type }) => ({ name, type, category: categorizeColumnType(type) }));
  } catch (error) {
    console.warn('[Webview] Failed to introspect column metadata', error);
    return fallbackColumns.map((name) => ({ name, type: 'UNKNOWN', category: 'other' }));
  }
}

function generateStableId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function formatTimestamp(timestamp: number): string {
  try {
    return new Date(timestamp).toLocaleTimeString(undefined, {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  } catch (error) {
    console.warn('[Webview] Failed to format timestamp', error);
    return new Date(timestamp).toISOString();
  }
}

function recordQueryHistory(sql: string) {
  const trimmed = sql.trim();
  if (!trimmed) {
    return;
  }

  const existingIndex = queryHistory.findIndex((entry) => entry.sql === trimmed);
  if (existingIndex !== -1) {
    queryHistory.splice(existingIndex, 1);
  }

  queryHistory.unshift({
    id: generateStableId('query'),
    sql: trimmed,
    timestamp: Date.now(),
  });

  if (queryHistory.length > MAX_HISTORY_ENTRIES) {
    queryHistory = queryHistory.slice(0, MAX_HISTORY_ENTRIES);
  }

  updateQueryHistoryList();
}

function updateQueryHistoryList() {
  if (!queryHistoryList) {
    return;
  }

  if (queryHistory.length === 0) {
    queryHistoryList.classList.add('empty');
    queryHistoryList.textContent = 'Run a query to start the time machine.';
    return;
  }

  queryHistoryList.classList.remove('empty');
  queryHistoryList.innerHTML = '';

  queryHistory.forEach((entry, index) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'history-entry';

    const label = document.createElement('div');
    label.className = 'history-entry__label';
    label.textContent = `${index === 0 ? 'Latest' : `#${index + 1}`} • ${formatTimestamp(entry.timestamp)}`;

    const sqlPreview = document.createElement('div');
    sqlPreview.className = 'history-entry__sql';
    sqlPreview.textContent = entry.sql.length > 600 ? `${entry.sql.slice(0, 597)}…` : entry.sql;

    button.append(label, sqlPreview);
    button.addEventListener('click', () => {
      if (sqlInput) {
        sqlInput.value = entry.sql;
      }
      runQuery(entry.sql).catch(reportError);
    });

    queryHistoryList.appendChild(button);
  });
}

function captureSnapshot() {
  if (!currentTableData || currentTableData.rows.length === 0) {
    updateStatus('Run a query with results before capturing a snapshot.');
    return;
  }

  if (!lastExecutedQuery.trim()) {
    updateStatus('Execute a query before capturing a snapshot.');
    return;
  }

  const entry: SnapshotEntry = {
    id: generateStableId('snapshot'),
    query: lastExecutedQuery,
    timestamp: Date.now(),
    rowCount: currentTableData.rows.length,
    sample: currentTableData.rows[0]?.display.join(' | ') ?? null,
    columns: [...currentTableData.columns],
  };

  snapshots = [entry, ...snapshots.filter((snapshot) => snapshot.query !== entry.query)];
  if (snapshots.length > MAX_SNAPSHOTS) {
    snapshots = snapshots.slice(0, MAX_SNAPSHOTS);
  }

  updateSnapshotList();
  renderHyperStatus('Snapshot captured!', 'Find it in the list below to relaunch that query universe.');
}

function updateSnapshotList() {
  if (!snapshotList) {
    return;
  }

  if (snapshots.length === 0) {
    snapshotList.classList.add('empty');
    snapshotList.textContent = 'Capture a snapshot to anchor a data universe.';
    return;
  }

  snapshotList.classList.remove('empty');
  snapshotList.innerHTML = '';

  snapshots.forEach((snapshot) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'snapshot-entry';

    const title = document.createElement('div');
    title.className = 'history-entry__label';
    title.textContent = `${snapshot.columns.length} columns • ${formatTimestamp(snapshot.timestamp)}`;

    const meta = document.createElement('div');
    meta.className = 'snapshot-entry__meta';
    const rowsSpan = document.createElement('span');
    rowsSpan.textContent = `${snapshot.rowCount.toLocaleString()} rows`;
    const hintSpan = document.createElement('span');
    hintSpan.textContent = 'Click to rerun';
    meta.append(rowsSpan, hintSpan);

    const sample = document.createElement('div');
    sample.className = 'snapshot-entry__sample';
    sample.textContent = snapshot.sample ? truncateForDisplay(snapshot.sample, 80) : 'No preview rows captured yet.';

    button.append(title, meta, sample);
    button.addEventListener('click', () => {
      if (sqlInput) {
        sqlInput.value = snapshot.query;
      }
      runQuery(snapshot.query).catch(reportError);
    });

    snapshotList.appendChild(button);
  });
}

async function handleHyperAction(action: string, button: HTMLButtonElement) {
  if (!activeRelation || !connection) {
    renderHyperStatus('Load a dataset first.', 'The hyperdrive needs a table from your file to chew on.');
    return;
  }

  if (!action) {
    renderHyperStatus('Unknown hyper action.', 'That control has not been wired into the warp core yet.');
    return;
  }

  button.disabled = true;

  try {
    if (action === 'profile') {
      await runHyperProfile();
      return;
    }

    if (action === 'nulls') {
      await runNullHeatmap();
      return;
    }

    if (action === 'duplicates') {
      await launchDuplicateHunter();
      return;
    }

    if (action === 'story') {
      await runRowStory();
      return;
    }

    renderHyperStatus(`Action '${action}' is still incubating.`, 'Ping the maintainers to wire it up.');
  } catch (error) {
    renderHyperError(error);
    throw error;
  } finally {
    button.disabled = false;
  }
}

async function runHyperProfile() {
  renderHyperStatus('Profiling columns…', 'Calculating dominant values, null storms, and averages in DuckDB.');
  const profile = await computeColumnProfile();
  cachedProfile = profile;
  renderProfileResults(profile);
}

async function runNullHeatmap() {
  renderHyperStatus('Measuring null storms…', 'Scanning every column for missing-value chaos.');
  if (!cachedProfile) {
    cachedProfile = await computeColumnProfile();
  }
  renderNullHeatmap(cachedProfile);
}

async function launchDuplicateHunter() {
  if (!activeRelation) {
    return;
  }
  const query = `
    SELECT *, COUNT(*) AS __duplicate_count
    FROM ${activeRelation.relationIdentifier}
    GROUP BY ALL
    HAVING COUNT(*) > 1
    ORDER BY __duplicate_count DESC
    LIMIT 200;
  `.trim();

  if (sqlInput) {
    sqlInput.value = query;
  }

  renderHyperStatus('Duplicate hunter unleashed.', 'The main preview now lists suspect clusters with their duplicate counts.');
  await runQuery(query);
}

async function runRowStory() {
  if (!activeRelation || !connection) {
    return;
  }
  renderHyperStatus('Sampling row stories…', 'Pulling a random cinematic slice of your table.');
  const query = `SELECT * FROM ${activeRelation.relationIdentifier} USING SAMPLE 12 ROWS;`;
  const table = await connection.query(query);
  const columns = table.schema.fields.map((field) => field.name);
  const rows: string[][] = [];
  for (let i = 0; i < table.numRows; i++) {
    const row = table.get(i) as Record<string, any> | null;
    if (!row) {
      continue;
    }
    rows.push(columns.map((column) => formatCell(row[column])));
  }
  renderHyperTable('Row Story Sampler', `Randomized ${rows.length} rows from ${activeRelation.relationName}.`, columns, rows);
}

async function computeColumnProfile(): Promise<ColumnProfileRow[]> {
  if (!activeRelation || !connection) {
    throw new Error('No active relation is ready for profiling yet.');
  }

  const metadata = activeRelation.metadata.length > 0
    ? activeRelation.metadata
    : activeRelation.columns.map((name) => ({ name, type: 'UNKNOWN', category: 'other' as ColumnCategory }));

  const statements = metadata.map((meta, index) => {
    const columnId = quoteIdentifier(meta.name);
    const topValueQuery = `(
      SELECT ${columnId}
      FROM ${activeRelation.relationIdentifier}
      WHERE ${columnId} IS NOT NULL
      GROUP BY ${columnId}
      ORDER BY COUNT(*) DESC
      LIMIT 1
    )`;
    const minExpr = meta.category === 'numeric' || meta.category === 'temporal'
      ? `MIN(${columnId})`
      : `MIN(CAST(${columnId} AS VARCHAR))`;
    const maxExpr = meta.category === 'numeric' || meta.category === 'temporal'
      ? `MAX(${columnId})`
      : `MAX(CAST(${columnId} AS VARCHAR))`;
    const avgExpr = meta.category === 'numeric'
      ? `AVG(TRY_CAST(${columnId} AS DOUBLE))`
      : 'NULL';
    const truthExpr = meta.category === 'boolean'
      ? `AVG(CASE WHEN ${columnId} IS TRUE THEN 1 ELSE 0 END)`
      : 'NULL';

    return `SELECT
      CAST(${index} AS INTEGER) AS column_position,
      ${escapeSqlLiteral(meta.name)} AS column_name,
      ${escapeSqlLiteral(meta.type)} AS data_type,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN ${columnId} IS NULL THEN 1 ELSE 0 END) AS null_count,
      COUNT(DISTINCT ${columnId}) AS distinct_count,
      ${topValueQuery} AS dominant_value,
      ${minExpr} AS min_value,
      ${maxExpr} AS max_value,
      ${avgExpr} AS average_value,
      ${truthExpr} AS truth_ratio
    FROM ${activeRelation.relationIdentifier}`;
  });

  const query = statements.join('\nUNION ALL\n');
  const table = await connection.query(query);
  const rows = table.toArray() as any[];

  return rows
    .map((row) => ({
      column_position: typeof row.column_position === 'number' ? row.column_position : Number(row.column_position ?? 0) || 0,
      column_name: String(row.column_name ?? row.COLUMN_NAME ?? ''),
      data_type: String(row.data_type ?? row.DATA_TYPE ?? 'UNKNOWN'),
      total_rows: typeof row.total_rows === 'number' ? row.total_rows : Number(row.total_rows ?? 0) || 0,
      null_count: typeof row.null_count === 'number' ? row.null_count : Number(row.null_count ?? 0) || 0,
      distinct_count: typeof row.distinct_count === 'number' ? row.distinct_count : Number(row.distinct_count ?? 0) || 0,
      dominant_value: row.dominant_value ?? null,
      min_value: row.min_value ?? null,
      max_value: row.max_value ?? null,
      average_value: typeof row.average_value === 'number'
        ? row.average_value
        : row.average_value !== null && row.average_value !== undefined
          ? Number(row.average_value)
          : null,
      truth_ratio: typeof row.truth_ratio === 'number'
        ? row.truth_ratio
        : row.truth_ratio !== null && row.truth_ratio !== undefined
          ? Number(row.truth_ratio)
          : null,
    }))
    .sort((a, b) => a.column_position - b.column_position);
}

function renderProfileResults(rows: ColumnProfileRow[]) {
  if (!hyperOutput) {
    return;
  }

  if (rows.length === 0) {
    renderHyperStatus('No profile statistics available.', 'The table appears to be empty.');
    return;
  }

  hyperOutput.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'hyper-output__title';
  title.textContent = `Column Profile (${rows.length} columns)`;

  const description = document.createElement('div');
  description.className = 'hyper-output__description';
  description.textContent = 'Distinct counts, null storms, dominant values, and averages directly from DuckDB.';

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  const headings = ['Column', 'Type', 'Distinct', 'Nulls', 'Null %', 'Dominant', 'Min', 'Max', 'Average', 'Truth %'];
  headings.forEach((heading) => {
    const th = document.createElement('th');
    th.textContent = heading;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    const nullRatio = row.total_rows > 0 ? row.null_count / row.total_rows : 0;
    const cells = [
      row.column_name,
      row.data_type,
      formatNumber(row.distinct_count),
      formatNumber(row.null_count),
      formatPercentage(nullRatio),
      formatHyperValue(row.dominant_value),
      formatHyperValue(row.min_value),
      formatHyperValue(row.max_value),
      row.average_value !== null && row.average_value !== undefined ? formatNumber(row.average_value, { maximumFractionDigits: 4 }) : '–',
      row.truth_ratio !== null && row.truth_ratio !== undefined ? formatPercentage(row.truth_ratio) : '–',
    ];
    cells.forEach((value) => {
      const td = document.createElement('td');
      td.textContent = value;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  hyperOutput.append(title, description, table);
}

function renderNullHeatmap(rows: ColumnProfileRow[]) {
  if (!hyperOutput) {
    return;
  }

  hyperOutput.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'hyper-output__title';
  title.textContent = 'Null Heatmap';

  const description = document.createElement('div');
  description.className = 'hyper-output__description';
  description.textContent = 'Columns ordered by their percentage of missing values.';

  const list = document.createElement('div');
  list.className = 'null-heatmap';

  let hasNulls = false;
  [...rows]
    .sort((a, b) => {
      const ratioA = a.total_rows > 0 ? a.null_count / a.total_rows : 0;
      const ratioB = b.total_rows > 0 ? b.null_count / b.total_rows : 0;
      return ratioB - ratioA;
    })
    .forEach((row) => {
      const ratio = row.total_rows > 0 ? row.null_count / row.total_rows : 0;
      if (ratio > 0) {
        hasNulls = true;
      }
      const item = document.createElement('div');
      item.className = 'null-heatmap__row';

      const meta = document.createElement('div');
      meta.className = 'null-heatmap__meta';
      const nameSpan = document.createElement('span');
      nameSpan.textContent = row.column_name;
      const valueSpan = document.createElement('span');
      valueSpan.textContent = `${formatPercentage(ratio)} (${formatNumber(row.null_count)} nulls)`;
      meta.append(nameSpan, valueSpan);

      const bar = document.createElement('div');
      bar.className = 'null-bar';
      const fill = document.createElement('div');
      fill.className = 'null-bar__fill';
      fill.style.width = `${Math.max(0, Math.min(100, ratio * 100))}%`;
      bar.appendChild(fill);

      item.append(meta, bar);
      list.appendChild(item);
    });

  hyperOutput.append(title, description);

  if (!list.hasChildNodes()) {
    const empty = document.createElement('div');
    empty.className = 'hyper-output__description';
    empty.textContent = 'No columns detected to chart. Did the file load correctly?';
    hyperOutput.append(empty);
    return;
  }

  hyperOutput.append(list);

  if (!hasNulls) {
    const celebratory = document.createElement('div');
    celebratory.className = 'hyper-output__description';
    celebratory.textContent = 'No nulls detected. This dataset is crystal clear!';
    hyperOutput.append(celebratory);
  }
}

function renderHyperTable(titleText: string, descriptionText: string, columns: string[], rows: string[][]) {
  if (!hyperOutput) {
    return;
  }

  hyperOutput.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'hyper-output__title';
  title.textContent = titleText;

  hyperOutput.appendChild(title);

  if (descriptionText) {
    const description = document.createElement('div');
    description.className = 'hyper-output__description';
    description.textContent = descriptionText;
    hyperOutput.appendChild(description);
  }

  if (rows.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'No rows returned.';
    hyperOutput.appendChild(empty);
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  columns.forEach((column) => {
    const th = document.createElement('th');
    th.textContent = column;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    row.forEach((cell) => {
      const td = document.createElement('td');
      td.textContent = cell;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  hyperOutput.appendChild(table);
}

function formatNumber(value: number | null | undefined, options?: Intl.NumberFormatOptions): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '–';
  }
  try {
    return Number(value).toLocaleString(undefined, options);
  } catch {
    return String(value);
  }
}

function formatPercentage(value: number | null | undefined, options?: { maximumFractionDigits?: number; multiply?: boolean }): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '–';
  }
  const maximumFractionDigits = options?.maximumFractionDigits ?? 1;
  const multiply = options?.multiply ?? true;
  const numeric = multiply ? value * 100 : value;
  try {
    return `${numeric.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits,
    })}%`;
  } catch {
    return `${numeric}%`;
  }
}

function formatHyperValue(value: any): string {
  const formatted = formatCell(value);
  return formatted ? truncateForDisplay(formatted) : '–';
}

function truncateForDisplay(text: string, maxLength = 64): string {
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1)}…`;
}

function renderHyperStatus(message: string, description?: string) {
  if (!hyperOutput) {
    return;
  }
  hyperOutput.innerHTML = '';
  const title = document.createElement('div');
  title.className = 'hyper-output__title';
  title.textContent = message;
  hyperOutput.appendChild(title);
  if (description) {
    const detail = document.createElement('div');
    detail.className = 'hyper-output__description';
    detail.textContent = description;
    hyperOutput.appendChild(detail);
  }
}

function renderHyperError(error: any) {
  const message = error instanceof Error ? error.message : String(error);
  renderHyperStatus('Hyper action exploded!', message);
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
